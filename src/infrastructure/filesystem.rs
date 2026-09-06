use std::io::Cursor;
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use image::codecs::jpeg::JpegEncoder;
use image::ImageReader;
use unicode_normalization::UnicodeNormalization;

use crate::application::ports::PreviewGenerator;
use crate::domain::FileKind;
use crate::{Result, TransferError};

pub(crate) const MAX_PREVIEW_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TOTAL_PREVIEW_BYTES: usize = 256 * 1024;
pub(crate) const MAX_FILE_COUNT: usize = 256;
pub(crate) const MAX_CHUNK_SIZE: usize = 256 * 1024;

pub(crate) struct ImagePreviewGenerator;

impl PreviewGenerator for ImagePreviewGenerator {
    fn image_thumbnail(&self, path: &Path) -> Result<Option<(String, Vec<u8>)>> {
        if file_kind(path) != FileKind::Image {
            return Ok(None);
        }
        let mut reader = ImageReader::open(path)
            .map_err(TransferError::Io)?
            .with_guessed_format()
            .map_err(TransferError::Io)?;
        let mut limits = image::Limits::default();
        limits.max_alloc = Some(32 * 1024 * 1024);
        limits.max_image_width = Some(16_384);
        limits.max_image_height = Some(16_384);
        reader.limits(limits);
        let image = reader
            .decode()
            .map_err(|error| TransferError::Invalid(format!("decode image preview: {error}")))?;
        let thumbnail = image.thumbnail(256, 256).to_rgb8();
        let mut encoded = Cursor::new(Vec::new());
        JpegEncoder::new_with_quality(&mut encoded, 78)
            .encode_image(&thumbnail)
            .map_err(|error| TransferError::Invalid(format!("encode image preview: {error}")))?;
        let bytes = encoded.into_inner();
        if bytes.len() > MAX_PREVIEW_BYTES {
            return Ok(None);
        }
        Ok(Some(("image/jpeg".into(), bytes)))
    }
}

pub(crate) fn preview_data_url(media_type: &str, bytes: &[u8]) -> Option<String> {
    (bytes.len() <= MAX_PREVIEW_BYTES).then(|| {
        format!(
            "data:{media_type};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )
    })
}

pub(crate) fn sanitize_relative_path(value: &str) -> Result<PathBuf> {
    if value.is_empty() || value.len() > 4096 || value.contains('\0') || value.contains('\\') {
        return Err(TransferError::Invalid("unsafe relative path".into()));
    }
    let components: Vec<_> = value.split('/').collect();
    if components.iter().any(|component| {
        component.is_empty()
            || component.len() > 255
            || *component == "."
            || *component == ".."
            || component.ends_with([' ', '.'])
            || component
                .chars()
                .any(|character| character.is_control() || r#"<>:"|?*"#.contains(character))
            || windows_reserved_name(component)
    }) || components
        .first()
        .is_some_and(|component| component.ends_with(':'))
    {
        return Err(TransferError::Invalid("unsafe path component".into()));
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(TransferError::Invalid(
            "absolute paths are not allowed".into(),
        ));
    }
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) if !value.is_empty() => safe.push(value),
            _ => return Err(TransferError::Invalid("unsafe path component".into())),
        }
    }
    if safe.as_os_str().is_empty() {
        return Err(TransferError::Invalid("empty relative path".into()));
    }
    Ok(safe)
}

pub(crate) fn portable_path_key(value: &str) -> String {
    value.nfc().flat_map(char::to_lowercase).collect()
}

fn windows_reserved_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end_matches([' ', '.'])
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9'))
}

pub(crate) fn unique_destination(root: &Path, relative: &Path) -> Result<PathBuf> {
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let name = relative
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| TransferError::Invalid("invalid file name".into()))?;
    let target_parent = root.join(parent);
    create_safe_directories(root, parent)?;
    let initial = target_parent.join(name);
    if !initial.exists() {
        return Ok(initial);
    }
    let stem = Path::new(name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    let extension = Path::new(name).extension().and_then(|value| value.to_str());
    for index in 1..10_000_u32 {
        let candidate_name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = target_parent.join(candidate_name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(TransferError::Invalid(
        "cannot choose unique destination".into(),
    ))
}

fn create_safe_directories(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    let root_metadata = std::fs::symlink_metadata(&current)?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(TransferError::Invalid(
            "receive directory must be a normal directory".into(),
        ));
    }
    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(TransferError::Invalid(
                    "destination contains a symlink or non-directory".into(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current)?;
                let metadata = std::fs::symlink_metadata(&current)?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(TransferError::Invalid(
                        "destination directory changed while receiving".into(),
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(crate) fn finalize_staged(root: &Path, relative: &Path, temporary: &Path) -> Result<PathBuf> {
    for _ in 0..10_000_u32 {
        let destination = unique_destination(root, relative)?;
        match std::fs::hard_link(temporary, &destination) {
            Ok(()) => {
                std::fs::remove_file(temporary)?;
                return Ok(destination);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(TransferError::Invalid(
        "cannot atomically finalize received file".into(),
    ))
}

pub(crate) fn file_kind(path: &Path) -> FileKind {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "heic" | "avif" => FileKind::Image,
        "pdf" => FileKind::Pdf,
        "zip" | "7z" | "rar" | "tar" | "gz" | "bz2" | "xz" => FileKind::Archive,
        _ => FileKind::File,
    }
}

pub(crate) fn media_type(path: &Path) -> &'static str {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "txt" | "md" => "text/plain",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_sanitizer_blocks_traversal_and_platform_paths() {
        for unsafe_path in ["", "../secret", "/tmp/file", "a/./b", "a//b", "C:\\secret"] {
            assert!(
                sanitize_relative_path(unsafe_path).is_err(),
                "{unsafe_path}"
            );
        }
        assert_eq!(
            sanitize_relative_path("folder/file.txt").unwrap(),
            PathBuf::from("folder/file.txt")
        );
    }

    #[test]
    fn media_type_uses_real_extension_case_insensitively() {
        assert_eq!(media_type(Path::new("photo.png")), "image/png");
        assert_eq!(media_type(Path::new("photo.JPG")), "image/jpeg");
        assert_eq!(media_type(Path::new("报告.PDF")), "application/pdf");
        assert_eq!(
            media_type(Path::new("没有扩展名")),
            "application/octet-stream"
        );
    }

    #[cfg(unix)]
    #[test]
    fn destination_directory_creation_does_not_follow_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        assert!(unique_destination(root.path(), Path::new("escape/new/file.txt")).is_err());
        assert!(!outside.path().join("new").exists());
    }
}

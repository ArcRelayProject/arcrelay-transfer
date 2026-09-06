use super::*;

pub(super) async fn prepare_files(
    manager: Arc<TransferManager>,
    transfer_id: String,
    paths: Vec<PathBuf>,
) -> Result<Vec<PreparedFile>> {
    let control = manager.job(&transfer_id).await?;
    let mut tasks = JoinSet::new();
    let mut previews_left = MAX_TOTAL_PREVIEW_BYTES / MAX_PREVIEW_BYTES;
    for (index, path) in paths.into_iter().enumerate() {
        let preview = file_kind(&path) == crate::domain::FileKind::Image && previews_left > 0;
        if preview {
            previews_left -= 1;
        }
        let resources = manager.resources.clone();
        let control = control.clone();
        tasks.spawn(async move {
            let work = resources
                .work(if preview { 64 * 1024 * 1024 } else { 1 })
                .await
                .map_err(|error| TransferError::InvalidState(error.to_string()))?;
            if control.is_cancelled() {
                return Err(TransferError::Cancelled);
            }
            tokio::task::spawn_blocking(move || {
                let _work = work;
                if control.is_cancelled() {
                    return Err(TransferError::Cancelled);
                }
                let prepared = prepare_file(index, path, preview)?;
                if control.is_cancelled() {
                    return Err(TransferError::Cancelled);
                }
                Ok(prepared)
            })
            .await
            .map_err(|e| TransferError::Invalid(format!("file preparation failed: {e}")))?
        });
    }
    let mut files = Vec::with_capacity(tasks.len());
    loop {
        let result = tokio::select! {
            _ = control.cancelled() => return Err(TransferError::Cancelled),
            _ = manager.stopping.cancelled() => return Err(TransferError::Cancelled),
            next = tasks.join_next() => next,
        };
        let Some(result) = result else {
            break;
        };
        files.push(result.map_err(|e| TransferError::Invalid(e.to_string()))??);
    }
    files.sort_by_key(|file| file.manifest.file_id);
    Ok(files)
}

fn prepare_file(index: usize, path: PathBuf, preview: bool) -> Result<PreparedFile> {
    let source = std::fs::File::open(&path)?;
    let metadata = source.metadata()?;
    if !metadata.is_file() {
        return Err(TransferError::Invalid(
            "source is not a regular file".into(),
        ));
    }
    // Preview failure is independent of whether the original bytes can be sent.
    let preview = if preview {
        match ImagePreviewGenerator.image_thumbnail(&path) {
            Ok(preview) => preview.map(|(media_type, data)| proto::TransferPreview {
                file_id: index as u64 + 1,
                media_type,
                data,
            }),
            Err(error) => {
                tracing::debug!(%error, "optional transfer preview skipped");
                None
            }
        }
    } else {
        None
    };
    Ok(PreparedFile {
        source: Some(source),
        source_modified: metadata.modified().ok(),
        manifest: proto::FileManifestEntry {
            file_id: index as u64 + 1,
            relative_path: file_name(&path)?,
            size: metadata.len(),
            media_type: media_type(&path).into(),

            modified_at_ms: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0),
        },
        preview,
    })
}

pub(super) fn build_offer(wire_id: u64, prepared: &[PreparedFile]) -> Result<proto::TransferOffer> {
    let mut offer = proto::TransferOffer {
        request_id: random_nonzero_u64(),
        transfer_id: wire_id,
        display_name: if prepared.len() == 1 {
            prepared[0].manifest.relative_path.clone()
        } else {
            format!("{} files", prepared.len())
        },
        files: prepared
            .iter()
            .map(|value| value.manifest.clone())
            .collect(),
        total_bytes: prepared.iter().map(|value| value.manifest.size).sum(),
        created_at_ms: now_ms(),
        previews: Vec::new(),
    };
    let mut preview_bytes = 0;
    for preview in prepared.iter().filter_map(|value| value.preview.clone()) {
        if preview_bytes + preview.data.len() > MAX_TOTAL_PREVIEW_BYTES {
            continue;
        }
        offer.previews.push(preview);
        if encoded_offer_frame_len(&offer) > MAX_CONTROL_FRAME_SIZE {
            offer.previews.pop();
            continue;
        }
        preview_bytes += offer.previews.last().map_or(0, |value| value.data.len());
    }
    if encoded_offer_frame_len(&offer) > MAX_CONTROL_FRAME_SIZE {
        return Err(TransferError::Invalid(
            "file manifest exceeds the control-frame limit; shorten file names or reduce the file count".into(),
        ));
    }
    debug_assert!(encoded_offer_frame_len(&offer) <= MAX_CONTROL_FRAME_SIZE);
    Ok(offer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_source_handle_survives_path_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shared.txt");
        std::fs::write(&path, b"prepared contents").unwrap();

        let mut prepared = prepare_file(0, path.clone(), false).unwrap();
        std::fs::rename(&path, directory.path().join("original.txt")).unwrap();
        std::fs::write(&path, b"replacement contents").unwrap();

        let mut contents = Vec::new();
        prepared
            .source
            .as_mut()
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"prepared contents");
    }
}

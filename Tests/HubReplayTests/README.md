# Hub replay tests

`HubReplayTests` checks public `HubClient` methods against recorded HTTP responses
on Swift 6.1 or later.
The current tests cover `getModelTags()` and `getDatasetTags()`.
They use separate Replay sessions, an explicit host, no token, and no Hub cache.

Playback matches the method and full URL without Hub access.
Missing fixtures and unmatched requests fail.
Strict playback and disabled recording are the defaults:

```sh
swift test --filter HubReplayTests
```

## Recordings

These reduced fixtures were captured on 2026-09-14 (UTC):

| Fixture | Method | URL |
|---|---|---|
| `model-tags.har` | GET | https://huggingface.co/api/models-tags-by-type |
| `dataset-tags.har` | GET | https://huggingface.co/api/datasets-tags-by-type |

The recording filter preserves complete entries within selected groups:

| Fixture | Group | Tag IDs |
|---|---|---|
| Model tags | `library` | `pytorch`, `transformers` |
| Model tags | `pipeline_tag` | `text-generation` |
| Dataset tags | `library` | `library:datasets` |
| Dataset tags | `language` | `language:en` |

It removes authentication headers, cookies, and stale body headers;
updates body sizes; and marks each HAR as reduced.
The fixtures cover selected tags and the unwrapped structure.

To replace a fixture with Hub access:

```sh
REPLAY_RECORD_MODE=rewrite REPLAY_PLAYBACK_MODE=strict swift test --filter HubReplayTests.TagsReplayTests/modelTags
REPLAY_RECORD_MODE=rewrite REPLAY_PLAYBACK_MODE=strict swift test --filter HubReplayTests.TagsReplayTests/datasetTags
```

For an initial capture, use `REPLAY_RECORD_MODE=once`.
Replay records beside the test source.
Playback checks there first, then the copied `Bundle.module` resources.

Before committing:

1. Check each HAR for one GET, the expected URL, and status 200.
2. Review the body diff and confirm that credentials and cookies are absent.
   Preserve retained fields; do not add a `tags` wrapper or invent entries.
3. Update the capture date and explain the change.
4. Run strict playback, then `swift test`.

To check bundle loading, move the source fixtures aside after building
and run `swift test --skip-build --filter HubReplayTests`.
A fixture missing from both locations or a wrong request URL must fail playback.
Restore all fixtures afterward.

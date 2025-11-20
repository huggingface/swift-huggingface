# Download Speed Test Example

This example demonstrates how to use the HuggingFace Swift package to download files from a repository and measure download performance. It's designed to compare download speeds with and without Xet support.

## Usage

### Running the Test

From the `Example` directory:

```bash
swift run download-speed-test
```

Use `--help` to see all arguments:

```bash
swift run download-speed-test --help
```

### Command Line Options

- `--repo <owner/name>` or `-r <owner/name>`: Repository to benchmark (default: Qwen/Qwen3-0.6B)
- `--file <path>` or `-f <path>`: Download a specific file (e.g., `model.safetensors`)
- `--min-size-mb <MB>`: Minimum file size in MB to test (default: 10, filters out small files)
- `--xet` / `--no-xet`: Enable or disable Xet acceleration

### Testing with Xet Enabled/Disabled

To compare performance with and without Xet:

**Basic comparison (auto-selects large files):**
```bash
# With Xet
swift run download-speed-test

# Without Xet (LFS)
swift run download-speed-test --no-xet
```

**Test a specific large file:**
```bash
# Download specific model file
swift run download-speed-test --file model.safetensors

# Compare Xet vs LFS for the same file
swift run download-speed-test --file model.safetensors --no-xet
```

**Test different repository:**
```bash
swift run download-speed-test --repo meta-llama/Llama-3.2-1B
```

**Adjust minimum file size filter:**
```bash
# Only test files >= 100 MB (better for Xet benchmarking)
swift run download-speed-test --min-size-mb 100

# Include smaller files (>= 1 MB)
swift run download-speed-test --min-size-mb 1
```

**Notes:**
- Xet excels at large files (10+ MB), so the benchmark filters out small files by default
- Small files (configs, JSONs) add overhead that doesn't showcase Xet's strengths
- Use `--file` to benchmark a specific large model file for accurate comparison

### Performance Features

Xet is optimized for high-performance downloads by default:

- **256 concurrent range GET requests** per file (automatically set)
- **High-performance mode enabled** for maximum throughput
- **XetClient reuse** across downloads for HTTP/TLS connection pooling
- **JWT token caching** per repository/revision to avoid redundant API calls

**No configuration required!** Xet should match or exceed LFS speeds out of the box.

If you need to adjust settings:

- **XET_NUM_CONCURRENT_RANGE_GETS**: Override default per-file concurrency
  ```bash
  XET_NUM_CONCURRENT_RANGE_GETS=128 swift run download-speed-test  # Lower for slow networks
  ```

- **XET_HIGH_PERFORMANCE**: Disable high-performance mode
  ```bash
  XET_HIGH_PERFORMANCE=0 swift run download-speed-test  # Conservative mode
  ```

## What It Does

The test:
1. Connects to the Hugging Face Hub
2. Lists files in the specified repository (default: `Qwen/Qwen3-0.6B`)
3. Selects large files (default: >= 10 MB) that showcase Xet's performance:
   - Model files (`.safetensors`, `.bin`, `.gguf`, `.pt`, `.pth`)
   - Prioritizes the largest files for meaningful benchmarking
4. Downloads each file and measures:
   - Download time
   - File size
   - Download speed (MB/s)
5. Provides a summary with total time, size, and average speed

**Why filter small files?**
Xet is optimized for large files through:
- Content-addressable storage with chunking
- Parallel chunk downloads
- Deduplication across files

Small files (<10 MB) don't benefit from these optimizations and add per-file overhead that skews results.

## Output Example

```
🚀 Hugging Face Download Speed Test
Repository: Qwen/Qwen3-0.6B
============================================================

✅ Xet support: ENABLED

📋 Listing files in repository...
📦 Selected 3 files for testing:
   • model.safetensors (1.2 GB)
   • model-00001-of-00002.safetensors (987 MB)
   • model-00002-of-00002.safetensors (256 MB)

⬇️  Starting download tests...

✅ [1/3] model.safetensors
   Time: 12.34s
   Size: 1.2 GB
   Speed: 99.2 MB/s

✅ [2/3] model-00001-of-00002.safetensors
   Time: 9.87s
   Size: 987 MB
   Speed: 100.1 MB/s

✅ [3/3] model-00002-of-00002.safetensors
   Time: 2.56s
   Size: 256 MB
   Speed: 100.0 MB/s

============================================================
📊 Summary
============================================================
Total files: 3
Total time: 24.77s
Total size: 2.4 GB
Average speed: 99.8 MB/s

💡 Tip: toggle Xet via --xet / --no-xet to compare backends.
```

## Notes

- The test uses a temporary directory that is automatically cleaned up
- Files are downloaded sequentially to get accurate timing
- The test automatically selects a mix of small and large files
- Progress is shown for each file download


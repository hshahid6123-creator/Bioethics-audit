# Plan: Markdown File Input Mode for Bioethics Audit Pipeline

## Goal
Add a `--markdown-dir` CLI mode so the pipeline can accept pre-existing markdown article files directly, bypassing PDF download (Phase 3) and PDF-to-Markdown conversion (Phase 4), while still running the multi-agent AI audit (Phase 5) and output (Phase 6).

## Context
- `run_audit.py` currently requires a journal archive URL to discover and download PDFs.
- The `convert_to_markdown()` function already caches output in `markdown_texts/{article_id}.md`, so the pipeline already operates on markdown content conceptually.
- **Critical gap**: PDF workflow gets metadata (title, authors, journal, year, pdf_url) from web crawling. Markdown input needs an alternative metadata source.

## Design Decisions

### 1. Metadata Sourcing (KEY DECISION)
**Recommended**: YAML frontmatter embedded at the top of each markdown file.

```markdown
---
article_title: "Original Article Title"
authors: "Author One, Author Two"
journal_name: "Journal of Rawalpindi Medical College"
year: 2023
pdf_url: "https://example.com/article.pdf"
---
## Article body starts here...
```

**Fallbacks** (applied in order):
1. YAML frontmatter (if present)
2. A JSON sidecar file `markdown_texts/_metadata.json` mapping article_id → metadata
3. Sensible defaults: filename-derived article_id, "Unknown" for missing fields, "N/A" for pdf_url

**Rationale**: YAML frontmatter is self-contained (one file = content + metadata), is a well-understood convention, and requires no external files. The JSON sidecar provides a simpler alternative for users who don't want to edit markdown.

### 2. CLI Interface
Add a new mutually-exclusive option group:
- `--markdown-dir <path>` — Directory containing `.md` files to process
- `--md-file <path>` — Single markdown file to process (alternative)

When these are used, the URL/file crawling phase (Phases 1-2) is skipped entirely.

### 3. article_id Generation
- From frontmatter `article_id` field if present
- Otherwise derived from the filename stem (e.g., `my_article.md` → `MD_my_article`)

### 4. Phase Skipping
When in markdown mode:
- Skip Phase 1 & 2 (discovery / crawling / metadata assembly)
- Skip Phase 3 (PDF download)
- Skip Phase 4 (PDF → Markdown conversion)
- Use the markdown content directly as `raw_markdown`
- Proceed to Phase 5 (multi-agent audit) unchanged
- Proceed to Phase 6 (CSV + SQLite output) unchanged

### 5. Circuit Breaker
Continue using the existing 30% error circuit breaker. Errors in markdown mode include: unreadable markdown file, missing frontmatter, audit failure, schema validation failure.

## Implementation Tasks

### Task 1: Add YAML frontmatter parsing
- Add lightweight frontmatter parser using `re` (no new dependency). If PyYAML is available, prefer it; otherwise parse simple `key: value` pairs.
- Create `load_markdown_file(md_path: Path) -> tuple[dict, str]` that returns `(metadata_dict, content_str)`
- Strip frontmatter delimiter lines from the content before passing to audit

### Task 2: Add markdown metadata assembly function
- Create `metadata_from_markdown(md_path: Path, fallback_smap: dict) -> dict` that:
  - Calls `load_markdown_file()` to get metadata + content
  - Merges with JSON sidecar metadata if present
  - Applies defaults for missing fields
  - Generates article_id if not in frontmatter
  - Returns a full metadata dict matching the structure used by `scraped_articles_metadata`

### Task 3: Add CLI argument parsing
- In `parse_args()`, add mutually exclusive group with `--markdown-dir` and `--md-file`
- Ensure this group is exclusive with `--url` and `--file`

### Task 4: Add markdown processing mode in `main()`
- If `--markdown-dir` or `--md-file` is provided:
  - Skip all discovery/crawling
  - Collect `.md` files from the directory (or use the single file)
  - Call `metadata_from_markdown()` for each file
  - Build the `queued` list directly (skip offset/limit or apply to file list)
  - In the execution loop, skip `download_pdf()` and `convert_to_markdown()`; use the loaded content directly
  - Log clearly that markdown mode is active

### Task 5: Handle pdf_url gracefully
- When `pdf_url` is "N/A" or missing, the audit should still work (the prompt only uses article text)
- Ensure `BioethicsRecord` validation passes with `pdf_url="N/A"`

### Task 6: Update help text and documentation
- Add a docstring note about markdown mode
- Consider adding a small example markdown with frontmatter

## Risks & Edge Cases
- **Missing metadata**: If frontmatter is absent and no sidecar exists, most fields will default. The AI audit may still work on content alone but results may be lower quality. This is acceptable.
- **Filename collisions**: If two files produce the same article_id, deduplication should use filename as tiebreaker.
- **Very large markdown files**: The existing 18,000-char truncation in `run_multi_agent_audit` still applies.
- **Non-markdown attachments**: If a user provides non-`.md` files in the directory, skip them with a warning.
- **yaml dependency**: Use a lightweight pure-Python frontmatter parser (regex-based) to avoid adding PyYAML as a new dependency. If PyYAML is already installed, use it for full YAML support; otherwise fall back to simple `key: value` parsing.

## Open Question
**Metadata format preference**: Is YAML frontmatter the preferred approach, or would you prefer:
- (a) A separate JSON metadata file (e.g., `metadata.json` alongside the markdown files)
- (b) Parsing metadata from markdown heading structure
- (c) Something else?

## Validation Plan
1. Create a sample markdown file with frontmatter and run `python run_audit.py --markdown-dir <dir> --limit 1`
2. Verify the article is audited and stored in SQLite + CSV with correct 16-column output
3. Test the no-frontmatter fallback path
4. Test with `--md-file` for a single file
5. Verify circuit breaker still functions in markdown mode

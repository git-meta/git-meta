#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTENT_DIR = ROOT / "content"
SITE_ROOT_DIR = ROOT / "docs"
BLOG_DIR = SITE_ROOT_DIR / "blog"
SITE_ORIGIN = "https://git-meta.com"


@dataclass(frozen=True)
class Post:
    slug: str
    source_path: Path
    output_path: Path
    title: str
    published: date
    description: str
    author: str
    body_markdown: str


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-") or "post"


def parse_front_matter(markdown: str) -> tuple[dict[str, str], str]:
    if not markdown.startswith("---\n"):
        return {}, markdown
    end = markdown.find("\n---\n", 4)
    if end == -1:
        return {}, markdown
    fields: dict[str, str] = {}
    for line in markdown[4:end].splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().lower()] = value.strip().strip('"\'')
    return fields, markdown[end + 5 :].lstrip()


def read_title(markdown: str, fallback: str) -> str:
    for line in markdown.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def first_paragraph(markdown: str) -> str:
    lines: list[str] = []
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped == "---":
            continue
        if not stripped:
            if lines:
                break
            continue
        lines.append(stripped)
    return " ".join(lines)


def inline_format(text: str) -> str:
    text = html.escape(text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", text)

    def link(match: re.Match[str]) -> str:
        label = match.group(1)
        url = html.escape(match.group(2), quote=True)
        return f'<a href="{url}">{label}</a>'

    return re.sub(r"\[([^\]]+)\]\(([^)]+)\)", link, text)


def markdown_to_html(markdown: str) -> tuple[str, list[tuple[int, str, str]]]:
    lines = markdown.splitlines()
    out: list[str] = []
    paragraph: list[str] = []
    list_items: list[str] = []
    in_code = False
    code_lines: list[str] = []
    headings: list[tuple[int, str, str]] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            out.append(f"<p>{inline_format(' '.join(paragraph).strip())}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            items = "".join(f"<li>{item}</li>" for item in list_items)
            out.append(f"<ul>{items}</ul>")
            list_items = []

    for line in lines:
        stripped = line.strip()
        if in_code:
            if stripped.startswith("```"):
                out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code = False
            else:
                code_lines.append(line)
            continue
        if stripped.startswith("```"):
            flush_paragraph()
            flush_list()
            in_code = True
            continue
        if not stripped:
            flush_paragraph()
            flush_list()
            continue
        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            flush_paragraph()
            flush_list()
            level = len(heading.group(1))
            text = heading.group(2).strip()
            if level == 1:
                continue
            anchor = slugify(text)
            text_html = inline_format(text)
            out.append(f'<h{level} id="{anchor}">{text_html}</h{level}>')
            if level in (2, 3, 4):
                headings.append((level, anchor, text_html))
            continue
        bullet = re.match(r"^[-*]\s+(.*)$", stripped)
        if bullet:
            flush_paragraph()
            list_items.append(inline_format(bullet.group(1)))
            continue
        paragraph.append(stripped)

    flush_paragraph()
    flush_list()
    if in_code:
        out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
    return "\n".join(out), headings


def build_toc(headings: list[tuple[int, str, str]]) -> str:
    if not headings:
        return ""
    items = []
    for level, anchor, text_html in headings:
        items.append(
            f'<li class="blog-toc-h{level}"><a href="#{html.escape(anchor)}">{text_html}</a></li>'
        )
    return (
        '<aside class="blog-toc" aria-label="On this page">'
        '<div class="blog-toc-title">On this page</div>'
        f'<ol>{"".join(items)}</ol>'
        '</aside>'
    )


def read_post(path: Path) -> Post:
    raw = path.read_text()
    fields, markdown = parse_front_matter(raw)
    title = fields.get("title") or read_title(markdown, path.stem.replace("-", " ").title())
    slug = fields.get("slug") or slugify(path.stem)
    published_text = fields.get("date")
    if published_text:
        published = date.fromisoformat(published_text)
    else:
        published = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).date()
    description = fields.get("description") or first_paragraph(markdown)
    author = fields.get("author", "")
    return Post(
        slug=slug,
        source_path=path,
        output_path=BLOG_DIR / slug / "index.html",
        title=title,
        published=published,
        description=description,
        author=author,
        body_markdown=markdown,
    )


def post_url(post: Post) -> str:
    return f"/blog/{post.slug}/"


def root_prefix(canonical_path: str) -> str:
    segments = [part for part in canonical_path.strip("/").split("/") if part]
    return "../" * len(segments)


def render_page(title: str, description: str, body: str, canonical_path: str) -> str:
    root = root_prefix(canonical_path)
    return f"""<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <script>
      (() => {{
        const stored = localStorage.getItem(\"git-meta-theme\");
        document.documentElement.dataset.theme = stored || \"system\";
      }})();
    </script>
    <title>{html.escape(title)} · git-meta</title>
    <meta name=\"description\" content=\"{html.escape(description, quote=True)}\" />
    <link rel=\"icon\" type=\"image/png\" href=\"{root}assets/git-meta-icon.png\" />
    <link rel=\"canonical\" href=\"{SITE_ORIGIN}{html.escape(canonical_path)}\" />
    <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\" />
    <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin />
    <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap\" rel=\"stylesheet\" />
    <link rel=\"stylesheet\" href=\"{root}styles.css\" />
  </head>
  <body>
    <header class=\"site-header\">
      <div class=\"wrap nav\">
        <a class=\"brand\" href=\"{root}index.html\" aria-label=\"git-meta home\">
          <img class=\"brand-mark\" src=\"{root}assets/git-meta-icon.png\" width=\"32\" height=\"32\" alt=\"\" />
          <span class=\"brand-name\">git-meta</span>
        </a>
        <nav class=\"nav-links\" aria-label=\"Primary\">
          <a href=\"{root}blog/\">Blog</a>
          <a href=\"{root}spec/\" class=\"nav-muted\">Spec &rarr;</a>
        </nav>
      </div>
    </header>
    {body}
  </body>
</html>
"""


def render_post(post: Post) -> str:
    content, headings = markdown_to_html(post.body_markdown)
    toc = build_toc(headings)
    author = f" / {html.escape(post.author)}" if post.author else ""
    body = f"""<main class=\"wrap blog-page blog-post-page\">
      <div class=\"blog-post-main\">
        <p class=\"blog-kicker\"><a href=\"../\">blog</a> / {post.published.isoformat()}{author}</p>
        <article class=\"blog-article\">
          <h1>{html.escape(post.title)}</h1>
          <div class=\"blog-content\">
            {content}
          </div>
        </article>
      </div>
      {toc}
    </main>"""
    return render_page(post.title, post.description, body, post_url(post))


def render_index(posts: list[Post]) -> str:
    cards = []
    for post in posts:
        display_date = f"{post.published.strftime('%B')} {post.published.day}, {post.published.year}"
        cards.append(
            f'<article class="blog-card"><time datetime="{post.published.isoformat()}">{display_date}</time>'
            f'<h2><a href="{post.slug}/">{html.escape(post.title)}</a></h2>'
            f'<p>{html.escape(post.description)}</p></article>'
        )
    body = f"""<main class=\"wrap blog-page blog-index\">
      <section class=\"blog-hero\">
        <p class=\"eyebrow\">Blog</p>
        <h1>project notes from git-meta</h1>
        <p>short little deep dives into how various things are handled with the git-meta approach to storing metadata with your git code</p>
      </section>
      <section class=\"blog-list\" aria-label=\"Posts\">
        {''.join(cards)}
      </section>
    </main>"""
    return render_page("blog", "project notes from git-meta.", body, "/blog/")


def generate_blog() -> None:
    if not CONTENT_DIR.exists():
        raise SystemExit(f"Missing content directory: {CONTENT_DIR}")
    posts = sorted(
        (read_post(path) for path in CONTENT_DIR.glob("*.md")),
        key=lambda post: (post.published, post.slug),
        reverse=True,
    )
    if BLOG_DIR.exists():
        shutil.rmtree(BLOG_DIR)
    BLOG_DIR.mkdir(parents=True, exist_ok=True)
    for post in posts:
        post.output_path.parent.mkdir(parents=True, exist_ok=True)
        post.output_path.write_text(render_post(post))
    (BLOG_DIR / "index.html").write_text(render_index(posts))
    print(f"Generated {len(posts)} blog posts in {BLOG_DIR}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate blog pages from content/*.md")
    return parser.parse_args()


def main() -> None:
    parse_args()
    generate_blog()


if __name__ == "__main__":
    main()

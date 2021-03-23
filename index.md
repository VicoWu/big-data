# Welcome to Chang Wu's Github Pages

You can use the [editor on GitHub](https://github.com/VicoWu/big-data/edit/gh-pages/index.md) to maintain and preview the content for your website in Markdown files.

Whenever you commit to this repository, GitHub Pages will run [Jekyll](https://jekyllrb.com/) to rebuild the pages in your site, from the content in your Markdown files.

* * *
- - -

## Presto Spill to Disk Deep Dive

By default, Presto kills queries if the memory requested by the query execution exceeds session properties `query_max_memory` or `query_max_memory_per_node`. This mechanism ensures fairness in allocation of memory to queries and prevents deadlock caused by memory allocation. It is efficient when there is a lot of small queries in the cluster, but leads to killing large queries that don’t stay within the limits.

To overcome this inefficiency, the concept of revocable memory was introduced. A query can request memory that does not count toward the limits, but this memory can be revoked by the memory manager at any time. When memory is revoked, the query runner spills intermediate data from memory to disk and continues to process it later.

Check the detail

* * *
- - -

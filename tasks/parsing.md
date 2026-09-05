# Parsing Optimizations

Currently, we are handrolling the parser for the requests. There are two things that we want to improve here:

1. The request header is being parsed twice. There is a potential optimization lurking there where we parse it only once.
2. I believe currently we are doing zero-copy parsing. But I think we are allocating a bunch of vectors in between. There lurks another possibility of optimization. Potentially, we want to explore whether there are standard solutions that Rust projects use to parse such protocols. Whether we need to use crates like `nom` or `winnow` or whether there is any code optimization we can do to improve the parsing performance.


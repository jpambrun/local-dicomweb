serve:
    deno run -A --watch server.mjs

crawl:
    deno run -A crawl.mjs

clean:
    rm dicomweb/*

compile:
    deno compile  --target=x86_64-pc-windows-msvc -A main.mjs

fmt:
    deno fmt --line-width=120
serve:
    deno run -A --watch server.mjs

crawl:
    deno run -A crawl.mjs

clean:
    rm dicomweb/*

compile:
    deno compile  -A --target=x86_64-pc-windows-msvc main.mjs --output local-dicomweb.exe

fmt:
    deno fmt --line-width=120

tag: compile
    #! /bin/bash
    tag=${TAG:-v$(date +%Y.%m.%d-%H%M%S)}
    echo "Creating and pushing tag: $tag"
    git tag -a "$tag" -m "Release $tag"
    git push origin "$tag"

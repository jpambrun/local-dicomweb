serve:
    bun run --hot server.ts

crawl:
    bun run crawl.ts

pack:
  bun run pack.ts

clean:
    rm -rf _dicomweb

compile:
    bun build --compile main.mjs --outfile local-dicomweb

fmt:
    bunx biome format --write .

check:
    bunx biome check .

tsc:
  bunx tsc

tag: compile
    #! /bin/bash
    tag=${TAG:-v$(date +%Y.%m.%d-%H%M%S)}
    echo "Creating and pushing tag: $tag"
    git tag -a "$tag" -m "Release $tag"
    git push origin "$tag"

# The SDK's checks, and the example shop (examples/shop.ts) as an image. Built from the repository root,
# for the .proto:  docker build -f sdk/Dockerfile -t ledgerline-shop .
FROM node:24.22.1-alpine
WORKDIR /app
COPY sdk/package.json sdk/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY sdk/ ./
COPY proto ./proto
# Type-checked and tested as part of the build.
RUN node_modules/.bin/tsc --noEmit && node --test "test/*.test.ts"
USER node
EXPOSE 8070
CMD ["node", "examples/shop.ts"]

# The dashboard: built with Vite, served by Caddy (which also forwards the API to ClickHouse).
FROM node:24.22.1-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN node_modules/.bin/vue-tsc --noEmit && node_modules/.bin/vite build

FROM caddy:2.11.7-alpine
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /src/dist /srv

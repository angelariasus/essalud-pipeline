FROM node:18-alpine
WORKDIR /app
COPY package.json pnpm-lock.yaml* ./
RUN corepack enable && pnpm install
COPY . .
EXPOSE 3000
CMD ["node", "index.js"]

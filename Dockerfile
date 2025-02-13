# First stage: Build the application
FROM node:16-alpine AS build
WORKDIR /app
COPY package*.json .
RUN npm install
COPY . .
RUN npm run build

# Second stage: Prepare the production image
FROM node:16-alpine
WORKDIR /app
COPY package.json .
RUN npm install --only=production
COPY --from=build /app/dist ./dist

EXPOSE 3000

CMD npm run start:prod

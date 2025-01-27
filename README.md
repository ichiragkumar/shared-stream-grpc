```markdown
# NestJS Microservice Application

This project demonstrates a NestJS application that integrates with a gRPC-based microservice and connects to a MongoDB database.

---

## Features

- **HTTP Server**: Hosts an HTTP server for API endpoints.
- **gRPC Microservice**: Connects to a gRPC server using a specified `.proto` file.
- **MongoDB Integration**: Establishes a connection to a MongoDB database using a custom database service.
- **Docker Support**: Multi-stage Dockerfile for building and running the application in a containerized environment.

---

## Setup Instructions

### Prerequisites

Ensure you have the following installed:

- Node.js (>= 16.x)
- npm (or yarn)
- Docker
- MongoDB (local or cloud instance)
- `.proto` file: The gRPC protocol definition file should be placed at `src/proto/coupon_stream.proto`.

---

### Installation

1. Clone the repository:

   ```bash
   git clone https://gitlab.com/a.hande/waw-grpc-server
   cd <project-directory>
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. Configure environment variables:

   Create a `.env` file in the root directory and add the following:

   ```env
   PORT=3000
   MONGODB_URI=mongodb://localhost:27017
   DB_NAME=couponDB
   GRPC_HOST=localhost
   GRPC_PORT=50051
   ```

---

### Running the Application Locally

1. Start the MongoDB server locally or use a cloud MongoDB URI.
2. Run the application:

   ```bash
   npm run start
   ```

3. The application will start:

   - HTTP server on `http://localhost:<PORT>` (default: `http://localhost:3000`)
   - gRPC server on `<GRPC_HOST>:<GRPC_PORT>` (default: `localhost:50051`)

---

### Using Docker

This project includes a multi-stage Dockerfile for building and running the application in a containerized environment.

#### Build and Run with Docker

1. Build the Docker image:

   ```bash
   docker build -t nestjs-microservice .
   ```

2. Run the Docker container:

   ```bash
   docker run -d --name nestjs-microservice \
     -p 3000:3000 \
     -e PORT=3000 \
     -e MONGODB_URI=mongodb://<your-mongo-host>:27017 \
     -e DB_NAME=couponDB \
     -e GRPC_HOST=localhost \
     -e GRPC_PORT=50051 \
     nestjs-microservice
   ```

3. The application will now be accessible on http://localhost:3000
4. The application for grpc will be accessible on  grpc://localhost:4005
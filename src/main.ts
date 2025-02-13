import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Server, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { DatabaseService } from './config/database.config';
import  { config } from 'dotenv';
import { addReflectionToGrpcConfig } from 'nestjs-grpc-reflection';

config();

async function bootstrap() {
  try {
    await DatabaseService.connect();
    console.log('Database connection established');
  } catch (error) {
    console.error('Failed to connect to database:', error);
    process.exit(1);
  }

  const app = await NestFactory.create(AppModule);

  // gRPC Microservice Configuration
  const grpcConfig: MicroserviceOptions = {
    transport: Transport.GRPC,
    options: {
      package: 'coupon',
      protoPath: join(__dirname, 'proto/coupon_stream.proto'),
      url: `${process.env.GRPC_HOST}:${process.env.PORT}`, // Use PORT for gRPC
    },
  };

  const grpcConfigWithReflection = addReflectionToGrpcConfig(grpcConfig);
  app.connectMicroservice(grpcConfigWithReflection);

  // Start all microservices
  await app.startAllMicroservices();

  // Start HTTP server
  await app.listen(Number(process.env.APP_PORT)); // Use APP_PORT for HTTP

  console.log(`HTTP server is running on port ${process.env.APP_PORT}`);
  console.log(`gRPC server is running on ${process.env.GRPC_HOST}:${process.env.PORT}`);
}

bootstrap();
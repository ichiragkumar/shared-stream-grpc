import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Server, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { DatabaseService } from './config/database.config';
import  { config } from 'dotenv';
import { GrpcReflectionModule } from 'nestjs-grpc-reflection';
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





//  app.connectMicroservice<MicroserviceOptions>({
//     transport: Transport.GRPC,
//     options: {
//       package: 'coupon',
//       protoPath: join(__dirname, 'proto/coupon_stream.proto'),
//       url: `${process.env.GRPC_HOST}:${process.env.GRPC_PORT}`,
//     },
//   });

  const grpcConfig: MicroserviceOptions = {
    transport: Transport.GRPC,
    options: {
      package: 'coupon',
      protoPath: join(__dirname, 'proto/coupon_stream.proto'),
      url: `${process.env.GRPC_HOST}:${process.env.GRPC_PORT}`,
    },
  };

  const grpcConfigWithReflection = addReflectionToGrpcConfig(grpcConfig);
  app.connectMicroservice(grpcConfigWithReflection);


  await app.startAllMicroservices();
  await app.listen(Number(process.env.PORT));



  console.log(`HTTP server running on http://localhost:${process.env.PORT}`);
  console.log(`gRPC server running on ${process.env.GRPC_HOST}:${process.env.GRPC_PORT}`);

}
bootstrap();

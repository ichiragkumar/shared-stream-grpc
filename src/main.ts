import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { DatabaseService } from './config/database.config';

async function bootstrap() {
  try {
    await DatabaseService.connect();
    console.log('Database connection established');
  } catch (error) {
    console.error('Failed to connect to database:', error);
    process.exit(1);
  }

  
  const app = await NestFactory.create(AppModule);
  

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: 'coupon',
      protoPath: join(__dirname, 'proto/coupon_stream.proto'),
      url: '0.0.0.0:4005',
    },
  });

  // Start both HTTP and gRPC services
  await app.startAllMicroservices();
  await app.listen(3000);
  
  console.log(`HTTP server running on http://localhost:3000`);
  console.log(`gRPC server running on 0.0.0.0:4005`);
}
bootstrap();

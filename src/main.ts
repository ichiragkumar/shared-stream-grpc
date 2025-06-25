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
    // Connect to database with optimized connection pool settings
    await DatabaseService.connect();
    console.log('Database connection established');
    
    // Monitor connection pool periodically with aggressive cleanup
    const monitorConnectionPool = async () => {
      try {
        const client = DatabaseService.getClient();
        const adminDb = client.db('admin');
        const serverStatus = await adminDb.command({ serverStatus: 1 });
        console.log('MongoDB connection pool stats:', serverStatus.connections);
        
        // Force garbage collection to clean up any lingering references
        try {
          if (typeof global.gc === 'function') {
            console.log('Running forced garbage collection...');
            global.gc();
          }
        } catch (error) {
          // Garbage collection not available, ignore
        }
        
        // If we're getting close to connection limit, force pool cleanup
        if (serverStatus.connections.current > 400) {
          console.warn('WARNING: Connection pool approaching limit, forcing cleanup');
          
          // Close idle connections aggressively
          if (typeof client.withSession === 'function') {
            const killCursor = client.db('admin').command({ killCursors: 1, cursors: [] });
            console.log('Attempted to kill idle cursors:', killCursor);
          }
        }
      } catch (error) {
        console.error('Failed to get connection stats:', error);
      }
    };
    
    // Check connection pool stats every minute
    setInterval(monitorConnectionPool, 60 * 1000);
    await monitorConnectionPool(); // Run once at startup
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
      channelOptions: {
      'grpc.keepalive_time_ms': 30000, // 30s ping interval
      'grpc.keepalive_timeout_ms': 10000, // 10s timeout after no ACK
      'grpc.keepalive_permit_without_calls': 1, // allow pings when no RPC
      'grpc.http2.max_pings_without_data': 0, // unlimited pings
      'grpc.http2.min_time_between_pings_ms': 10000,
      'grpc.http2.min_ping_interval_without_data_ms': 5000,
    },
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
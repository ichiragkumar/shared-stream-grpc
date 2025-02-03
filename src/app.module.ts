import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CouponModule } from './coupon/coupon.module';
import { BusinessModule } from './business/business.module';
import { AuthModule } from './auth/auth.module';
import { GrpcReflectionModule } from 'nestjs-grpc-reflection';
import { join } from 'path';
import { Transport } from '@nestjs/microservices';

@Module({
  imports: [CouponModule, BusinessModule, AuthModule,
    GrpcReflectionModule.register({
      transport: Transport.GRPC,
      options: {
        package: 'coupon',
        protoPath: join(__dirname, 'proto/coupon_stream.proto'),
        url: `${process.env.GRPC_HOST}:${process.env.GRPC_PORT}`,
      },
    }),

  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}

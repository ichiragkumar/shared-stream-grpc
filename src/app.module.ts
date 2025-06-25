import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CouponModule } from './coupon/coupon.module';
import { AuthModule } from './auth/auth.module';
import { LoggerModule } from './logger/logger.module';
import { ConnectionManagerService } from './config/connection-manager.service';
import { SharedChangeStreamService } from './config/shared-change-stream.service';

@Module({
  imports: [CouponModule, AuthModule, LoggerModule],
  controllers: [AppController],
  providers: [
    AppService,
    ConnectionManagerService,
    SharedChangeStreamService 
  ],
})
export class AppModule {}

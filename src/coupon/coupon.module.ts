import { Module } from '@nestjs/common';
import { CouponController } from './coupon.controller';
import { CouponService } from './coupon.service';
import { CouponGrpcController } from './coupon.grpc.controller';
import { LoggerModule } from 'src/logger/logger.module';
import { ConnectionManagerService } from '../config/connection-manager.service';
import { SharedChangeStreamService } from '../config/shared-change-stream.service';

@Module({
  imports: [LoggerModule],
  controllers: [CouponController, CouponGrpcController],
  providers: [CouponService, ConnectionManagerService, SharedChangeStreamService]
})
export class CouponModule {}

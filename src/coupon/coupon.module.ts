import { Module } from '@nestjs/common';
import { CouponController } from './coupon.controller';
import { CouponService } from './coupon.service';
import { CouponGrpcController } from "./coupon.grpc.controller"

@Module({
  controllers: [CouponController,CouponGrpcController],
  providers: [CouponService]
})
export class CouponModule {}

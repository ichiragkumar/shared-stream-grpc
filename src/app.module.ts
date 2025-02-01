import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CouponModule } from './coupon/coupon.module';
import { BusinessModule } from './business/business.module';
import { AuthModule } from './auth/auth.module';

@Module({
  imports: [CouponModule, BusinessModule, AuthModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}

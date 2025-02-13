import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { CouponModule } from './coupon/coupon.module';
import { AuthModule } from './auth/auth.module';
import { LoggerModule } from './logger/logger.module';


@Module({
  imports: [CouponModule, AuthModule,LoggerModule ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}

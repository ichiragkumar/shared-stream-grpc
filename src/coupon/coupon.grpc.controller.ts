import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { Observable } from 'rxjs';
import { CouponService } from './coupon.service';
import { 
  StatusFilter, 
  CouponIssue, 
  ExpiredCouponFilter, 
  ExpiredCouponIssue,
  UserFilter,
  MoreCouponRequest,
  CouponStatusFilter,
  UserCoupon
} from "../generated/coupon_stream";

@Controller()
export class CouponGrpcController {
  constructor(private readonly couponService: CouponService) {}

  @GrpcMethod('CouponStreamService')
  StreamCouponIssues(data: StatusFilter): Observable<CouponIssue> {
    console.log('StreamCouponIssues called with data:', data);
    return this.couponService.streamCouponIssues(data);
  }

  @GrpcMethod('CouponStreamService')
  StreamExpiredCoupons(data: ExpiredCouponFilter): Observable<ExpiredCouponIssue> {
    return this.couponService.streamExpiredCoupons(data);
  }

  @GrpcMethod('CouponStreamService')
  StreamMoreCouponRequests(data: UserFilter): Observable<MoreCouponRequest> {
    return this.couponService.streamMoreCouponRequests(data);
  }

  @GrpcMethod('CouponStreamService')
  GetCouponsByStatus(data: CouponStatusFilter): Observable<UserCoupon> {
    return this.couponService.getCouponsByStatus(data);
  }
}
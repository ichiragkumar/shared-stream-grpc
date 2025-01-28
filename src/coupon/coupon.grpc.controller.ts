import { Controller } from '@nestjs/common';
import { GrpcMethod, GrpcStreamMethod } from '@nestjs/microservices';
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
  UserCoupon,
  CouponIssueWithBusiness,
  EmptyRequest
} from "../generated/coupon_stream";

@Controller()
export class CouponGrpcController {
  constructor(private readonly couponService: CouponService) {}

  @GrpcMethod('CouponStreamService', 'StreamCouponIssues')
  StreamCouponIssues(data: StatusFilter): Observable<CouponIssue> {
    return this.couponService.streamCouponIssues(data);
  }

  @GrpcMethod('CouponStreamService')
  StreamExpiredCoupons(data: ExpiredCouponFilter): Observable<ExpiredCouponIssue> {
    return this.couponService.streamExpiredCoupons(data);
  }

  @GrpcMethod('CouponStreamService', 'StreamMoreCouponRequests')
  StreamMoreCouponRequests(data: UserFilter): Observable<MoreCouponRequest> {
    return this.couponService.streamMoreCouponRequests(data);
  }

  @GrpcMethod('CouponStreamService')
  GetCouponsByStatus(data: CouponStatusFilter): Observable<UserCoupon> {
    return this.couponService.getCouponsByStatus(data);
  }



  @GrpcMethod('CouponStreamService', 'ActiveCouponIssuesWithBusinessesStream')
  ActiveCouponIssuesWithBusinessesStream(): Observable<CouponIssueWithBusiness> {
  return this.couponService.streamActiveCouponIssuesWithBusiness();
  }

}
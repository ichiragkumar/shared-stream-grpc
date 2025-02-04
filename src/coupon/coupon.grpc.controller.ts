import { BadRequestException, Controller, UseGuards } from '@nestjs/common';
import { GrpcMethod, GrpcStreamMethod } from '@nestjs/microservices';
import { Observable, of, throwError } from 'rxjs';
import { CouponService } from './coupon.service';
import { 
  StatusFilter, 
  CouponIssue, 
  UserFilter,
  MoreCouponRequest,
  CouponIssueWithBusiness,
  ActiveBusinessesStreamResponse,
  ActiveCouponStreamResponse,
  Balance,
  UserPrefrences,
  WalletBalanceResponse,
  ActiveDrawnResponse,
  User
} from "../generated/coupon_stream";




@Controller()
export class CouponGrpcController {
  constructor(private readonly couponService: CouponService) {}

  @GrpcMethod('CouponStreamService', 'StreamCouponIssues')
  StreamCouponIssues(data: UserPrefrences): Observable<CouponIssue> {
    return this.couponService.streamCouponIssuesService(data);
  }

  
  @GrpcMethod('CouponStreamService', 'ActiveCouponIssuesWithBusinessesStream')
  ActiveCouponIssuesWithBusinessesStream(data: UserPrefrences): Observable<CouponIssueWithBusiness> {
    console.log("aactive coupon issue with business stream controller "); 
    return this.couponService.streamActiveCouponIssuesWithBusinessService(data);
  }


  @GrpcMethod('CouponStreamService', 'StreamActiveBusinessesStream')
  streamActiveBusinessesStream(data: UserPrefrences): Observable<ActiveBusinessesStreamResponse> {
    return this.couponService.streamActiveBusinessesWithContractTypesService(data);
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveCoupons')
  streamActiveCouponsStream(data: User): Observable<ActiveCouponStreamResponse> {
    return this.couponService.streamActiveCouponsStreamService(data);
  }


  @GrpcMethod('CouponStreamService', 'StreamMoreCouponRequests')
    StreamMoreCouponRequests(data: User): Observable<MoreCouponRequest> {
    return this.couponService.streamMoreCouponRequestsService(data);
  }

  @GrpcMethod('CouponStreamService', 'WalletStream')
  streamWalletController(data: User): Observable<WalletBalanceResponse> {
    return this.couponService.streamWalletService(data);
  }


  @GrpcMethod("CouponStreamService", "StreamActiveDrawn")
  StreamActiveDrawn(data:UserPrefrences): Observable<ActiveDrawnResponse> {
    return this.couponService.streamActiveDrawnService(data)
  }
  

}
import { Controller } from '@nestjs/common';
import { Observable } from 'rxjs';
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
  User,
  TicketStreamResponse,
  ZoneStreamResponse,
  BusinessBranchStreamResponse,
  UserCartStreamResponse,
  UserCartStreamItem,
  UserNotificationStreamResponse,
  MainUser,
  EmptyRequest,
  EnvironmentResponse,
} from '../generated/coupon_stream';
import { LoggerService } from '../logger/logger.service';
import { Metadata } from '@grpc/grpc-js';
import { GrpcMethod } from '@nestjs/microservices';

@Controller()
export class CouponGrpcController {
  constructor(
    private readonly couponService: CouponService,
    private readonly logger: LoggerService,
  ) {}

  @GrpcMethod('CouponStreamService', 'StreamCouponIssues')
  StreamCouponIssues(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<CouponIssue> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'StreamCouponIssues',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamCouponIssuesService(data).subscribe({
        next: (couponIssue) => {
          this.logger.logStreamEvent(requestId, 'COUPON_ISSUE', {
            issueId: couponIssue.id,
            businessId: couponIssue.businessId,
            status: couponIssue.status,
          });
          subscriber.next(couponIssue);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'ActiveCouponIssuesWithBusinessesStream')
  ActiveCouponIssuesWithBusinessesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<CouponIssueWithBusiness> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'ActiveCouponIssuesWithBusinessesStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService
        .streamActiveCouponIssuesWithBusinessService(data)
        .subscribe({
          next: (couponIssue) => {
            this.logger.logStreamEvent(requestId, 'ACTIVE_COUPON_ISSUE', {
              issueId: couponIssue.couponIssueId,
              businessId: couponIssue.businessId,
              status: couponIssue.status,
            });
            subscriber.next(couponIssue);
          },
          error: (error) => {
            this.logger.logError(requestId, error);
            subscriber.error(error);
          },
          complete: () => {
            this.logger.finalizeRequest(requestId);
            subscriber.complete();
          },
        });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveBusinessesStream')
  streamActiveBusinessesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ActiveBusinessesStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'StreamActiveBusinessesStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService
        .streamActiveBusinessesWithContractTypesService(data)
        .subscribe({
          next: (businessResponse) => {
            this.logger.logStreamEvent(requestId, 'ACTIVE_BUSINESS', {
              businessId: businessResponse.id,
              contractType: businessResponse.contractType,
              title: businessResponse.title,
            });
            subscriber.next(businessResponse);
          },
          error: (error) => {
            this.logger.logError(requestId, error);
            subscriber.error(error);
          },
          complete: () => {
            this.logger.finalizeRequest(requestId);
            subscriber.complete();
          },
        });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveCoupons')
  streamActiveCouponsStream(
    data: User,
    metadata: Metadata,
  ): Observable<ActiveCouponStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'StreamActiveCoupons',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamActiveCouponsStreamService(data).subscribe({
        next: (couponResponse) => {
          this.logger.logStreamEvent(requestId, 'ACTIVE_COUPON', {
            businessId: couponResponse.businessId,
            code: couponResponse.code,
          });
          subscriber.next(couponResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamMoreCouponRequests')
  StreamMoreCouponRequests(
    data: User,
    metadata: Metadata,
  ): Observable<MoreCouponRequest> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = data.userId || (metadata.get('user-id')?.[0] as string);

    const requestId = this.logger.initializeRequest(
      'StreamMoreCouponRequests',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamMoreCouponRequestsService(data).subscribe({
        next: (moreCouponRequest) => {
          this.logger.logStreamEvent(requestId, 'MORE_COUPON_REQUEST', {
            requestId: moreCouponRequest.id,
            userId: moreCouponRequest.userId,
            couponIssueId: moreCouponRequest.couponIssueId,
            createdAt: moreCouponRequest.createdAt,
          });
          subscriber.next(moreCouponRequest);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'WalletStream')
  streamWalletController(
    data: User,
    metadata: Metadata,
  ): Observable<WalletBalanceResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'WalletStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamWalletService(data).subscribe({
        next: (walletBalance) => {
          this.logger.logStreamEvent(requestId, 'WALLET_BALANCE_UPDATE', {
            userId: data.userId,
            balance: walletBalance,
          });
          subscriber.next(walletBalance);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveDrawn')
  StreamActiveDrawn(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ActiveDrawnResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'StreamActiveDrawn',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamActiveDrawnService(data).subscribe({
        next: (activeDrawn) => {
          this.logger.logStreamEvent(requestId, 'ACTIVE_DRAWN_UPDATE', {
            drawnId: activeDrawn.id,
            businessId: activeDrawn.businessId,
            status: activeDrawn.status,
          });
          subscriber.next(activeDrawn);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'TicketsStream')
  TicketsStream(
    data: User,
    metadata: Metadata,
  ): Observable<TicketStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'TicketsStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.TicketsStreamService(data).subscribe({
        next: (ticketStreamResponse) => {
          this.logger.logStreamEvent(requestId, 'TICKET_STREAM', {
            id: ticketStreamResponse.id,
            userId: ticketStreamResponse.userId,
            drawId: ticketStreamResponse.drawId,
            drawType: ticketStreamResponse.drawType,
            isDrawClosed: ticketStreamResponse.isDrawClosed,
            drawNumbers: ticketStreamResponse.drawNumbers,
            createdAt: ticketStreamResponse.createdAt,
            status: ticketStreamResponse.status,
            streamType: ticketStreamResponse.streamType,
          });
          subscriber.next(ticketStreamResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'ZonesStream')
  ZonesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ZoneStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'ZonesStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.ZonesStreamService(data).subscribe({
        next: (zoneStreamResponse) => {
          this.logger.logStreamEvent(requestId, 'ZONE_STREAM', {
            id: zoneStreamResponse.id,
            country: zoneStreamResponse.country,
            createdAt: zoneStreamResponse.createdAt,
            isDefault: zoneStreamResponse.isDefault,
            name: zoneStreamResponse.name,
            location: zoneStreamResponse.location,
            streamType: zoneStreamResponse.streamType,
          });
          subscriber.next(zoneStreamResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'BusinessBranchStream')
  BusinessBranchStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<BusinessBranchStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'BusinessBranchStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.BusinessBranchStreamService(data).subscribe({
        next: (businessBranchResponse) => {
          this.logger.logStreamEvent(requestId, 'BUSINESS_BRANCH', {
            id: businessBranchResponse.id,
            businessSuspended: businessBranchResponse.businessSuspended,
            shortAddress: businessBranchResponse.shortAddress,
            businessId: businessBranchResponse.businessId,
            zoneId: businessBranchResponse.zoneId,
            location: businessBranchResponse.location,
            openingHours: businessBranchResponse.openingHours,
            createdAt: businessBranchResponse.createdAt,
            contractTypes: businessBranchResponse.contractTypes,
            streamType: businessBranchResponse.streamType,
          });
          subscriber.next(businessBranchResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamUserCarts')
  StreamUserCarts(
    data: User,
    metadata: Metadata,
  ): Observable<UserCartStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'UserCartStreamResponse',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.UserCartStreamResponseService(data).subscribe({
        next: (response: UserCartStreamResponse) => {
          if (response?.items?.length) {
            const filteredItems = response.items.filter(
              (item) => item.itemId && item.amount,
            );

            if (filteredItems.length) {
              const streamType = filteredItems[0].streamType;

              subscriber.next({
                items: filteredItems,
                streamType,
              });

              this.logger.logStreamEvent(
                requestId,
                'USER_CART_STREAM_RESPONSE',
                {
                  totalItems: filteredItems.length,
                  streamType,
                  items: filteredItems.map((item) => ({
                    itemId: item.itemId,
                    amount: item.amount,
                    purchasePrice: item.purchasePrice,
                    currency: item.currency,
                    feePrice: item.feePrice || 0,
                    taxAmount: item.taxAmount || 0,
                  })),
                },
              );
            }
          }
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'StreamUserNotifications')
  StreamUserNotifications(
    data: MainUser,
    metadata: Metadata,
  ): Observable<UserNotificationStreamResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = data
      ? data.userId || (metadata.get('user-id')?.[0] as string)
      : '';

    const requestId = this.logger.initializeRequest(
      'StreamUserNotifications',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.streamUserNotificationsService(data).subscribe({
        next: (userNotificationStreamResponse) => {
          this.logger.logStreamEvent(requestId, 'USER_NOTIFICATION', {
            id: userNotificationStreamResponse.id,
            isRead: userNotificationStreamResponse.isRead,
            createdAt: userNotificationStreamResponse.createdAt,
            title: userNotificationStreamResponse.title,
            body: userNotificationStreamResponse.body,
            topic: userNotificationStreamResponse.topic,
            screen: userNotificationStreamResponse.screen,
            userId: userNotificationStreamResponse.userId,
            streamType: userNotificationStreamResponse.streamType,
          });
          subscriber.next(userNotificationStreamResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }

  @GrpcMethod('CouponStreamService', 'EnvironmentStream')
  environmentStream(
    request: EmptyRequest,
    metadata: Metadata,
  ): Observable<EnvironmentResponse> {
    const userAgent = (metadata.get('user-agent')?.[0] as string) || 'Unknown';
    const ipAddress = (metadata.get('ip-address')?.[0] as string) || 'Unknown';
    const dns = (metadata.get('dns')?.[0] as string) || 'Unknown';
    const userId = metadata.get('user-id')?.[0] as string;

    const requestId = this.logger.initializeRequest(
      'EnvironmentStream',
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber) => {
      this.couponService.environmentStreamService().subscribe({
        next: (environmentResponse) => {
          this.logger.logStreamEvent(requestId, 'ENVIRONMENT', {
            id: environmentResponse.id,
            allowInvites: environmentResponse.allowInvites,
            allowInviteAll: environmentResponse.allowInviteAll,
            stage: environmentResponse.stage,
            deleteUnsentReports: environmentResponse.deleteUnsentReports,
            useCrashlytics: environmentResponse.useCrashlytics,
            auditLogsCredentials: environmentResponse.auditLogsCredentials,
            requiredMinimumAndroidVersion:
              environmentResponse.requiredMinimumAndroidVersion,
            requiredMiliumOSVersion:
              environmentResponse.requiredMinimumiOSVersion,
            deleteAndroidUnsentReports:
              environmentResponse.deleteAndroidUnsentReports,
            deleteiOSUnsentReports: environmentResponse.deleteiOSUnsentReports,
            useAndroidCrashlytics: environmentResponse.useAndroidCrashlytics,
            useiOSCrashlytics: environmentResponse.useiOSCrashlytics,
            streamType: environmentResponse.streamType,
          });
          subscriber.next(environmentResponse);
        },
        error: (error) => {
          this.logger.logError(requestId, error);
          subscriber.error(error);
        },
        complete: () => {
          this.logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }
}

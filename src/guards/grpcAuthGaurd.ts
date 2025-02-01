// import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
// import { Observable } from 'rxjs';
// import * as jwt from 'jsonwebtoken';
// import { config } from 'dotenv';
// import { Metadata } from '@grpc/grpc-js';

// config(); 

// @Injectable()
// export class GrpcAuthGuard implements CanActivate {
//   canActivate(context: ExecutionContext): boolean | Promise<boolean> | Observable<boolean> {
//     const metadata: Metadata = context.getArgByIndex(1);




//     const JWT_SECRET = process.env.JWT_SECRET;
//     if (!JWT_SECRET) {
//       throw new UnauthorizedException('JWT_SECRET is not set');
//     }


//     const authHeaders = metadata.get('authorization');
//     if (!authHeaders || authHeaders.length === 0) {
//       throw new UnauthorizedException('No token provided');
//     }

//     const token = authHeaders[0].toString();

//     try {
//       const decoded = jwt.verify(token, JWT_SECRET);
//       context.switchToRpc().getContext().user = decoded;
//       return true;
//     } catch (error) {
//       throw new UnauthorizedException('Invalid or expired token');
//     }
//   }
// }

import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { Observable } from 'rxjs';
import * as jwt from 'jsonwebtoken';
import { config } from 'dotenv';
import { Metadata } from '@grpc/grpc-js';
import { DatabaseService } from 'src/config/database.config';
import { Db } from 'mongodb';

config();

@Injectable()
export class GrpcAuthGuard implements CanActivate {
  private db: Db;
    
  constructor() {}
  
  async onModuleInit() {
      this.db = await DatabaseService.connect();
  }
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const metadata: Metadata = context.getArgByIndex(1);
    const JWT_SECRET = process.env.JWT_SECRET;

    if (!JWT_SECRET) {
      throw new UnauthorizedException('JWT_SECRET is not set');
    }

    const authHeaders = metadata.get('authorization');
    if (!authHeaders || authHeaders.length === 0) {
      throw new UnauthorizedException('No token provided');
    }

    const token = authHeaders[0].toString();

    try {
      // Verify the token and decode it
      const decoded = jwt.verify(token, JWT_SECRET) as { phoneNumber: string };
      
      // Now check if the phone number from the token exists in the database
      const user = await this.db.collection('users').findOne({ phoneNumber: decoded.phoneNumber });
      
      if (!user) {
        throw new UnauthorizedException('Invalid user or user does not exist');
      }

      // Add user information to the request context
      context.switchToRpc().getContext().user = decoded;

      return true;
    } catch (error) {
      throw new UnauthorizedException('Invalid or expired token');
    }
  }
}


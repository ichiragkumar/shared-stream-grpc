import { Body, Controller, Post } from '@nestjs/common';
import * as jwt from 'jsonwebtoken';
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';

@Controller('api/v1/auth')
export class AuthController {

    private db: Db;
    
    constructor() {}
    
    async onModuleInit() {
        this.db = await DatabaseService.connect();
    }


    @Post('token')
    generateToken(@Body('phoneNumber') phoneNumber: string) {
      const JWT_SECRET = process.env.JWT_SECRET || 'YOUR_JWT_SECRET';

      if (!phoneNumber) {
        return {
            error: 'Phone number is required',
            status: 400
        }
      }
  
      const isPhoneNumberRegistered =  this.db.collection('users').findOne({ phoneNumber });
      if (!isPhoneNumberRegistered) {
        return {
            error: 'Phone number is not registered',
            status: 400
        }
      }
      const token = jwt.sign({ phoneNumber }, JWT_SECRET, { expiresIn: '1h' });
      return { token };
    }
  }
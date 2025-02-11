import { Injectable, BadRequestException, InternalServerErrorException } from '@nestjs/common';
import { Db, Filter } from 'mongodb';
import { Language } from 'src/config/constant';
import { DatabaseService } from 'src/config/database.config';
import { PrizeDocument, safeParseDate } from 'src/types';

export interface PrizeResponse {
  id: string;
  amount: number;
  rank: number;
  value: number;
  title: string;
  image: string | null;
  description: string;
  drawId: string;

}

@Injectable()
export class PrizeService {
  private db: Db;

  constructor() {}

  async onModuleInit() {
      this.db = await DatabaseService.connect();
  }

  async getPrizeData(drawId: string, languageCode: string, brightness: string): Promise<PrizeResponse | { message: string }> {
    try {


     const filterId: Filter<PrizeDocument> = { _id: drawId } as Filter<PrizeDocument>;
     const prizeDocument = await this.db
            .collection<PrizeDocument>('drawPrizes')
            .findOne(filterId);
    

      if (!prizeDocument) {
        return { message: 'No prize data found for the provided drawId.' };
      }

      return this.mapPrizeResponse(prizeDocument, languageCode, brightness);
    } catch (error) {
      throw new InternalServerErrorException('Failed to fetch prize data. : getPrizeData');
    }
  }

  private mapPrizeResponse(prize: PrizeDocument, languageCode: string, brightness: string): PrizeResponse {
    return {
      id: prize._id?.toString() || '',
      amount: prize.amount ?? 0,
      rank: prize.rank ?? 0,
      value: prize.value ?? 0,
      title: prize.title?.[languageCode] || prize.title?.[Language.DEFAULT] || 'Unknown',
      image: prize.image?.[brightness]?.[languageCode] || prize.image?.[brightness]?.[Language.DEFAULT] || null,
      description: prize.description?.[languageCode] || prize.description?.[Language.DEFAULT] || 'No description available',
      drawId: prize.drawId || ''
    };
  }
}

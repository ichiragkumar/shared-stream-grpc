import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { PrizeService } from './prize.service';
import { Brightness, Language } from 'src/config/constant';

@Controller('api/v1/prize')
export class PrizeController {

    constructor(private readonly prizeService: PrizeService) {}

    @Get('/prize-data')
    async getPrizeData(
        @Query('drawId') drawId: string,
        @Query('languageCode') languageCode: string = Language.DEFAULT,
        @Query('brightness') brightness: string = Brightness.DEFAULT
    ) {
        if (!drawId) {
            throw new BadRequestException('Invalid request: drawId is required.');
        }

        return this.prizeService.getPrizeData(drawId, languageCode, brightness);
    }

}

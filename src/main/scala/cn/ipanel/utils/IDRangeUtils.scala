package cn.ipanel.utils

import cn.ipanel.common.EProgSrcType._
import cn.ipanel.common.IDRange._
import cn.ipanel.common.IDRangeConstant._

/**
  * id分段处理工具<br>
  * ${DESCRIPTION}
  *
  * @author liujjy
  * @create 2018/6/29
  * @since 1.0.0
  */
object IDRangeUtils {


  /**
    * 通过idf判断是图文还是应用
    *
    * @param programId
    * @return
    */
  def isPhotoOrApp(programId: Long): Boolean = {
    (programId > APP_START && programId < APP_END) || (programId > PHOTO_START && programId < PHOTO_EDN)
  }

  /**
    * 通过ID获取节目源类型
    *
    * @param nProgramID
    * @return
    */
  def getTheTypeOfProgram(nProgramID: Long): Int = {
    var nProgramType = 0
    if (nProgramID >= PROGRAM_VIDEO_ID_BEGIN && nProgramID <= PROGRAM_VIDEO_ID_END) {
      nProgramType = PROGRAM_VIDEO_TYPE
    } else if (nProgramID >= PROGRAM_EVENT_ID_BEGIN && nProgramID <= PROGRAM_EVENT_ID_END) {
      nProgramType = PROGRAM_EVENT_TYPE
    }
    else if (nProgramID >= PROGRAM_APP_ID_BEGIN && nProgramID <= PROGRAM_APP_ID_END) {
      nProgramType = PROGRAM_APP_TYPE
    }
    else if (nProgramID >= PROGRAM_CHANNEL_BEGIN && nProgramID <= PROGRAM_CHANNEL_END) {
      nProgramType = PROGRAM_CHANNEL_TYPE
    }
    else if (nProgramID >= PROGRAM_SERIES_VIDEO_ID_BEGIN && nProgramID <= PROGRAM_SERIES_VIDEO_ID_END) {
      nProgramType = PROGRAM_SERIES_VIDEO_TYPE
    }
    else if (nProgramID >= PROGRAM_SERIES_EVENT_ID_BEGIN && nProgramID <= PROGRAM_SERIES_EVENT_ID_END) {
      nProgramType = PROGRAM_SERIES_EVENT_TYPE
    }
    else if (nProgramID >= PROGRAM_MUSIC_ID_BEGIN && nProgramID <= PROGRAM_MUSIC_ID_END) {
      nProgramType = PROGRAM_MUSIC_TYPE
    }
    else if (nProgramID >= PROGRAM_MUSIC_SINGER_ID_BEGIN && nProgramID <= PROGRAM_MUSIC_SINGER_ID_END) {
      nProgramType = PROGRAM_MUSIC_SINGER_TYPE
    }
    else if (nProgramID >= PROGRAM_MUSIC_ALBUM_ID_BEGIN && nProgramID <= PROGRAM_MUSIC_ALBUM_ID_END) {
      nProgramType = PROGRAM_MUSIC_ALBUM_TYPE
    }
    else if (nProgramID >= PROGRAM_NEWS_ID_BEGIN && nProgramID <= PROGRAM_NEWS_ID_END) {
      nProgramType = PROGRAM_NEWS_TYPE
    }
    else if (nProgramID >= PROGRAM_DUPLICATE_ID_BEGIN && nProgramID <= PROGRAM_DUPLICATE_ID_END) {
      nProgramType = PROGRAM_DUPLICATE_TYPE
    }
    else if (nProgramID >= PROGRAM_MONITOR_BEGIN && nProgramID <= PROGRAM_MONITOR_END) {
      nProgramType = PROGRAM_MONITOR_TYPE
    }
    else if (nProgramID >= PROGRAM_MOSAIC_CHANNEL_BEGIN && nProgramID <= PROGRAM_MOSAIC_CHANNEL_END) {
      nProgramType = PROGRAM_MOSAIC_TYPE
    }
    else if (nProgramID >= PROGRAM_MOSAIC_SET_ID_BEGIN && nProgramID <= PROGRAM_MOSAIC_SET_ID_END) {
      nProgramType = PROGRAM_MOSAIC_TYPE
    }
    else if (nProgramID >= PROGRAM_LOCAL_CHANNEL_BEGIN && nProgramID <= PROGRAM_LOCAL_CHANNEL_END) {
      nProgramType = PROGRAM_LOCAL_CHANNEL_TYPE
    }
    //新增商铺、活动、电商主产品三种节目
    else if (nProgramID >= PROGRAM_SHOP_ID_BEGIN && nProgramID <= PROGRAM_SHOP_ID_END) {
      nProgramType = PROGRAM_SHOPPING_SHOP_TYPE
    }
    else if (nProgramID >= PROGRAM_PROMO_ID_BEGIN && nProgramID <= PROGRAM_PROMO_ID_END) {
      nProgramType = PROGRAM_SHOPPING_PROMO_TYPE
    }
    else if (nProgramID >= PROGRAM_MAIN_PRODUCT_ID_BEGIN && nProgramID <= PROGRAM_MAIN_PRODUCT_ID_END) {
      nProgramType = PROGRAM_SHOPPING_PRODUCT_TYPE
    }
    //新增旅游类媒资
    else if (nProgramID >= TOURISM_ROUTE_ID_BEGIN && nProgramID <= TOURISM_ROUTE_ID_END) {
      nProgramType = PROGRAM_TOURISM_ROUTE_TYPE
    }
    else if (nProgramID >= TOURISM_TICKET_ID_BEGIN && nProgramID <= TOURISM_TICKET_ID_END) {
      nProgramType = PROGRAM_TOURISM_TICKET_TYPE
    }
    //新增专题类媒资
    else if (nProgramID >= SUBJECT_ID_BEGIN && nProgramID <= SUBJECT_ID_END) {
      nProgramType = PROGRAM_SUBJECT_TYPE
    }
    //! 直播间
    else if (nProgramID >= PROGRAM_LIVE_ROOM_ID_BEGIN && nProgramID <= PROGRAM_LIVE_ROOM_ID_END) {
      nProgramType = PROGRAM_LIVE_ROOM_TYPE
    }
    //! 直播间回看
    else if (nProgramID >= PROGRAM_LIVE_PROGRAM_ID_BEGIN && nProgramID <= PROGRAM_LIVE_PROGRAM_ID_END) {
      nProgramType = PROGRAM_LIVE_PROGRAM_TYPE
    }
    //新增播单类媒资
    else if (nProgramID >= PLAYLIST_ID_BEGIN && nProgramID <= PLAYLIST_ID_END) {
      nProgramType = PROGRAM_PLAYLIST_TYPE
    }
    // 明星百科
    else if (nProgramID >= PROGRAM_STAR_ID_BEGIN && nProgramID <= PROGRAM_STAR_ID_END) {
      nProgramType = PROGRAM_STAR_TYPE
    } else {
      nProgramType = PROGRAM_UNKNOWN_TYPE
    }

    nProgramType
  }
}

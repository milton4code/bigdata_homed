package cn.ipanel.common

/**
  * 用户行为类型
  *
  * @author liujjy  
  * @date 2017/12/26 15:33
  */

object UserActionType {

  /** 切换/播放节目 */
  val SWITCH_OR_PLAY = "1"

  /** 媒资库存类型点播 */
  val MEDIA_ON_DEMOND = "1"

  /** 媒资库存类型回看 */
  val MEDIA_LOOK_BACK = "2"

  /** 用户行为看上集 */
  val USER_ACTION_LOOK_LAST = "26"

  /** 营业收入优惠券购买方式,支付方式大于10000表示优惠券购买 */
  val BUSINESS_DISCOUNT_COUPON = "10000"

  /** 用户订购且支付成功 */
  val BUSINESS_USER_PAY_SUCCESS = "1"

  /** 业务营收类型（消费）*/
  val BUSINESS_ORDER_TYPE_CONSUME="1"

  /** 套餐状态:65002（表示套餐正在发布(生效)状态中）*/
  val BUSINESS_PACKAGE_EFFECTIVE="65002"
}

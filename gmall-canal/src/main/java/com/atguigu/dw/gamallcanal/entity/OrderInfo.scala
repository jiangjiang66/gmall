package com.atguigu.dw.gamallcanal.entity

import java.util.Date

case class OrderInfo(id:Int,
                     consignee:String,   //收货人
                     consignee_tel:String, //收件人电话
                     total_amount:String,  //总金额
                     order_status:String,  //订单状态
                     user_id:String,  //用户id
                     payment_way:String,  //付款方式
                     delivery_address:String,  //送货地址
                     order_comment:String, //订单备注
                     out_trade_no:String,  //订单交易编号
                     trade_body:String,  //订单描述
                     create_time:Date, //创建时间
                     operate_time:Date, //操作时间
                     expire_time:Date,  //失效时间
                     tracking_no:String, //物流单编号
                     parent_order_id:String, //父订单编号
                     img_url:String,  // 图片路径
                     province_id:String //地区
                    ){

}

package bd.compare

import java.util.Properties
import collection.mutable._
import org.apache.spark.sql.{ SparkSession, Row}
import org.apache.spark.sql.types._
import bd.Utils


/**
  *
  * 很重要，请注意——
  * 此代码为验证、急速扩展开发，需要舍弃和重构。
  * 新增功能请勿参考、拓展此代码。
  * 切切！
  *
  **/


object Education {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("spark://隐藏")
      .appName("隐藏")
      .config("spark.executor.memory", "8g")
      .getOrCreate()

    val mysqlUrl = "jdbc:mysql://隐藏/bigdata?useSSL=false"
    val mysqlProps = new Properties
    mysqlProps.setProperty("user", "root")
    mysqlProps.setProperty("password", "隐藏")

    //  各专业职位数量初始值
    var yydzjs = 0 // 应用电子技术
    var yydzjsMap:Map[String, Int] = Map()
    var yydzjsMapCQ:Map[String, Int] = Map()
    val yydzjsSet = Set("电子技术", "计算机硬件", "半导体", "医疗设备", "器械", "集成电路", "仪器仪表", "工业自动化", "计算机服务")
    
    var jsjyyjs = 0 // 网络技术 <- 计算机应用技术
    var jsjyyjsMap:Map[String, Int] = Map()
    var jsjyyjsMapCQ:Map[String, Int] = Map()
    val jsjyyjsSet = Set("计算机应用", "计算机服务", "计算机硬件", "网络工程", "维护")
    
    var rjjs = 0 // 软件技术
    var rjjsMap:Map[String, Int] = Map()
    var rjjsMapCQ:Map[String, Int] = Map()
    val rjjsSet = Set("计算机软件", "互联网", "移动互联网", "IT互联网", "网络游戏", "IT服务")
    
    var yjsjsyyy = 0 // 云计算技术与应用
    var yjsjsyyyMap:Map[String, Int] = Map()
    var yjsjsyyyMapCQ:Map[String, Int] = Map()
    val yjsjsyyySet = Set("云计算", "大数据", "IT互联网",  "数据服务")
    
    var ydyykf = 0 // 移动应用开发
    var ydyykfMap:Map[String, Int] = Map()
    var ydyykfMapCQ:Map[String, Int] = Map()
    val ydyykfSet = Set("移动互联网", "计算机软件", "网络游戏")
    
    var wlwyyjs = 0 // 物联网应用技术
    var wlwyyjsMap:Map[String, Int] = Map()
    var wlwyyjsMapCQ:Map[String, Int] = Map()
    val wlwyyjsSet = Set("物联网", "计算机硬件", "维护")
    
    var txjs = 0 // 通信技术
    var txjsMap:Map[String, Int] = Map()
    var txjsMapCQ:Map[String, Int] = Map()
    val txjsSet = Set("通信", "网络设备", "电信运营", "电信", "网络工程", "运营商", "增值服务", "维护")
    
    var jzzsgcjs = 0 // 建筑装饰工程技术
    var jzzsgcjsMap:Map[String, Int] = Map()
    var jzzsgcjsMapCQ:Map[String, Int] = Map()
    val jzzsgcjsSet = Set("建筑装饰", "工程", "装修装饰", "建筑", "装潢", "装饰装潢", "建材", "家居", "木材", "木材及竹", "灯饰照明", "玻璃", "塑料", "陶瓷")
    
    var jzsnsj = 0 // 建筑室内设计
    var jzsnsjMap:Map[String, Int] = Map()
    var jzsnsjMapCQ:Map[String, Int] = Map()
    val jzsnsjSet = Set("室内设计", "灯饰照明", "装潢", "装修", "家居", "玻璃", "塑料", "陶瓷")
    
    var jzgcjs = 0 // 建筑工程技术
    var jzgcjsMap:Map[String, Int] = Map()
    var jzgcjsMapCQ:Map[String, Int] = Map()
    val jzgcjsSet = Set("建筑", "工程", "建筑工程", "建材", "原材料和加工")
    
    var jsgcgl = 0 // 建设工程管理
    var jsgcglMap:Map[String, Int] = Map()
    var jsgcglMapCQ:Map[String, Int] = Map()
    val jsgcglSet = Set("建筑", "工程", "建筑工程", "工程管理")
    
    var gczj = 0 // 工程造价
    var gczjMap:Map[String, Int] = Map()
    var gczjMapCQ:Map[String, Int] = Map()
    val gczjSet = Set("工程造价", "建筑工程", "工程管理", "建筑", "工程")
    
    var fdcjyygl = 0 // 房地产经营与管理
    var fdcjyyglMap:Map[String, Int] = Map()
    var fdcjyyglMapCQ:Map[String, Int] = Map()
    val fdcjyyglSet = Set("房地产", "房地产开发", "市场推广", "租赁服务", "租赁")
    
    var csgdjtgcjs = 0 // 城市轨道交通工程技术
    var csgdjtgcjsMap:Map[String, Int] = Map()
    var csgdjtgcjsMapCQ:Map[String, Int] = Map()
    val csgdjtgcjsSet = Set("交通", "城市轨道", "城市交通", "地质")
    
    var skjs = 0 // 数控技术
    var skjsMap:Map[String, Int] = Map()
    var skjsMapCQ:Map[String, Int] = Map()
    val skjsSet = Set("数控", "数控技术", "设备", "机械", "工业自动化", "电气自动化", "机电一体化")
    
    var jzdqgcjs = 0 // 建筑电气工程技术
    var jzdqgcjsMap:Map[String, Int] = Map()
    var jzdqgcjsMapCQ:Map[String, Int] = Map()
    val jzdqgcjsSet = Set("电力", "建筑电气", "建筑电气工程", "化工")
    
    var jdythjs = 0 // 机电一体化技术
    var jdythjsMap:Map[String, Int] = Map()
    var jdythjsMapCQ:Map[String, Int] = Map()
    val jdythjsSet = Set("机电", "机电设备", "机电一体化", "机械", "化工")
    
    var dqzdhjs = 0 // 电气自动化技术
    var dqzdhjsMap:Map[String, Int] = Map()
    var dqzdhjsMapCQ:Map[String, Int] = Map()
    val dqzdhjsSet = Set("电气", "电力", "电气自动化", "工业自动化", "检测", "仪器仪表及工业自动化")
    
    var jdsbwxygl = 0 // 机电设备维修与管理
    var jdsbwxyglMap:Map[String, Int] = Map()
    var jdsbwxyglMapCQ:Map[String, Int] = Map()
    val jdsbwxyglSet = Set("机电", "机电设备", "机电设备维修", "机电设备管理", "设备", "重工", "租赁服务", "租赁")
    
    var gyjqrjs = 0 // 工业机器人技术
    var gyjqrjsMap:Map[String, Int] = Map()
    var gyjqrjsMapCQ:Map[String, Int] = Map()
    val gyjqrjsSet = Set("机器人", "工业机器人", "工业自动化", "电气自动化", "汽车制造", "数控")
    
    var qcjcywxjs = 0 // 汽车检测与维修技术
    var qcjcywxjsMap:Map[String, Int] = Map()
    var qcjcywxjsMapCQ:Map[String, Int] = Map()
    val qcjcywxjsSet = Set("汽车维修", "汽车检测", "汽车及零配件", "电子技术", "仪器仪表", "设备")
    
    var qczzyzpjs = 0 // 汽车制造与装配技术
    var qczzyzpjsMap:Map[String, Int] = Map()
    var qczzyzpjsMapCQ:Map[String, Int] = Map()
    val qczzyzpjsSet = Set("汽车制造", "汽车装配", "数控", "数控技术", "仪器仪表", "设备", "电气自动化", "工业自动化")
    
    var xnyqcjs = 0 // 新能源汽车技术
    var xnyqcjsMap:Map[String, Int] = Map()
    var xnyqcjsMapCQ:Map[String, Int] = Map()
    val xnyqcjsSet = Set("新能源", "新能源汽车", "新能源汽车研发", "汽车及零配件")
    
    var qcyxyfw = 0 // 汽车营销与服务
    var qcyxyfwMap:Map[String, Int] = Map()
    var qcyxyfwMapCQ:Map[String, Int] = Map()
    val qcyxyfwSet = Set("汽车营销", "汽车服务", "汽车销售", "租赁服务", "租赁", "市场推广", "专业服务", "检测")
    
    var zqyqh = 0 // 证券与期货
    var zqyqhMap:Map[String, Int] = Map()
    var zqyqhMapCQ:Map[String, Int] = Map()
    val zqyqhSet = Set("证券", "期货", "基金", "投资", "金融", "银行", "拍卖", "典当", "信托", "担保")
    
    var kj = 0 // 会计
    var kjMap:Map[String, Int] = Map()
    var kjMapCQ:Map[String, Int] = Map()
    val kjSet = Set("会计", "审计", "税务", "财会", "金融" , "银行", "贸易")
    
    var scyx = 0 // 市场营销
    var scyxMap:Map[String, Int] = Map()
    var scyxMapCQ:Map[String, Int] = Map()
    val scyxSet = Set("市场营销", "市场推广", "保险", "贸易", "商业中心")
    
    var gjsw = 0 // 国际商务
    var gjswMap:Map[String, Int] = Map()
    var gjswMapCQ:Map[String, Int] = Map()
    val gjswSet = Set("国际商务", "电子商务", "贸易", "进出口")
    
    var dzsw = 0 // 电子商务
    var dzswMap:Map[String, Int] = Map()
    var dzswMapCQ:Map[String, Int] = Map()
    val dzswSet = Set("电子商务", "互联网", "移动互联网", "批发", "零售", "化妆品")
    
    var swyy = 0 // 商务英语
    var swyyMap:Map[String, Int] = Map()
    var swyyMapCQ:Map[String, Int] = Map()
    val swyySet = Set("商务英语", "贸易", "进出口", "贸易", "教育", "培训")
    
    var wlgl = 0 // 物流管理
    var wlglMap:Map[String, Int] = Map()
    var wlglMapCQ:Map[String, Int] = Map()
    val wlglSet = Set("物流", "运输", "物流管理", "贸易", "交通")
    
    var gsqygl = 0 // 工商企业管理
    var gsqyglMap:Map[String, Int] = Map()
    var gsqyglMapCQ:Map[String, Int] = Map()
    val gsqyglSet = Set("工商管理", "企业管理", "产业管理", "人力资源", "咨询", "专业服务", "认证")
    
    var lsjygl = 0 // 连锁经营管理
    var lsjyglMap:Map[String, Int] = Map()
    var lsjyglMapCQ:Map[String, Int] = Map()
    val lsjyglSet = Set("连锁经营", "连锁管理", "消费品", "酒店", "餐饮", "餐饮业", "休闲", "家具", "家电", "礼品", "批发", "零售", "饮料", "烟酒", "食品")
    
    var lygl = 0 // 旅游管理
    var lyglMap:Map[String, Int] = Map()
    var lyglMapCQ:Map[String, Int] = Map()
    val lyglSet = Set("旅游管理", "旅游", "度假", "酒店", "文化娱乐")
    
    var jdgl = 0 // 酒店管理
    var jdglMap:Map[String, Int] = Map()
    var jdglMapCQ:Map[String, Int] = Map()
    val jdglSet = Set("酒店", "酒店管理", "旅游", "度假", "公关", "餐饮")
    
    var hzchygl = 0 // 会展策划与管理
    var hzchyglMap:Map[String, Int] = Map()
    var hzchyglMapCQ:Map[String, Int] = Map()
    val hzchyglSet = Set("会展", "会展策划", "会展管理", "文化传播", "政府", "非盈利机构")
    
    var whscjygl = 0 // 文化市场经营管理
    var whscjyglMap:Map[String, Int] = Map()
    var whscjyglMapCQ:Map[String, Int] = Map()
    val whscjyglSet = Set("文化市场", "学术", "科研", "文化市场经营", "文化市场管理", "会展", "文化", "文化传播", "出版", "艺术")
    
    var gbysjmzz = 0 // 广播影视节目制作
    var gbysjmzzMap:Map[String, Int] = Map()
    var gbysjmzzMapCQ:Map[String, Int] = Map()
    val gbysjmzzSet = Set("影视", "媒体", "广告", "传媒", "出版", "文化传播", "文化")
    
    var ysdh = 0 // 影视动画
    var ysdhMap:Map[String, Int] = Map()
    var ysdhMapCQ:Map[String, Int] = Map()
    val ysdhSet = Set("影视", "动画", "文化", "文化传播")
    
    var hjyssj = 0 // 环境艺术设计
    var hjyssjMap:Map[String, Int] = Map()
    var hjyssjMapCQ:Map[String, Int] = Map()
    val hjyssjSet = Set("环境艺术", "艺术", "环境设计", "文化", "环保")
    
    var cpyssj = 0 // 产品艺术设计
    var cpyssjMap:Map[String, Int] = Map()
    var cpyssjMapCQ:Map[String, Int] = Map()
    val cpyssjSet = Set("艺术", "产品艺术", "工艺品", "珠宝", "奢侈品", "文化")
    
    var fzyfssj = 0 // 服装与服饰设计
    var fzyfssjMap:Map[String, Int] = Map()
    var fzyfssjMapCQ:Map[String, Int] = Map()
    val fzyfssjSet = Set("服装", "纺织", "皮革", "服饰", "市场营销")
    
    var sjcbsjyzz = 0 // 视觉传播设计与制作
    var sjcbsjyzzMap:Map[String, Int] = Map()
    var sjcbsjyzzMapCQ:Map[String, Int] = Map()
    val sjcbsjyzzSet = Set("视觉传播设计", "视觉传播制作", "广告")
    
    var szmtyssj = 0 // 数字媒体艺术设计
    var szmtyssjMap:Map[String, Int] = Map()
    var szmtyssjMapCQ:Map[String, Int] = Map()
    val szmtyssjSet = Set("媒体", "数字媒体", "文字媒体", "影视")
    
    var dmsj = 0 // 动漫设计
    var dmsjMap:Map[String, Int] = Map()
    var dmsjMapCQ:Map[String, Int] = Map()
    val dmsjSet = Set("动漫", "动画", "动漫设计", "影视", "文化", "文化传播")
    
    var jzdhymszz = 0  // 建筑动画与模式制作
    var jzdhymszzMap:Map[String, Int] = Map()
    var jzdhymszzMapCQ:Map[String, Int] = Map()
    val jzdhymszzSet = Set("建筑","建筑动画", "建筑动画制作", "建筑模式制作")
    
    val utils = new Utils
    
    //    获取职位数据
    var yesterday = utils.getYesterday
    val yesterdayDF = ss.read.jdbc(mysqlUrl, "bigdata_job", Array("date_='" + yesterday + "'"), mysqlProps)
    println(yesterdayDF.count())

    for (row <- yesterdayDF.collect()) {
      var categorySet = row.getAs("category").toString().split(",").toSet

      var education = row.getAs("education")
        .toString().replace(" ", "")
        .toUpperCase.trim()

      if (education.contains("高中")) education = "高中"
      else if (education.contains("中专")) education = "中专"
      else if (education.contains("中技")) education = "中技"
      else if (education.contains("大专") || education.contains("高职")) education = "大专"
      else if (education.contains("本科") || education.contains("学士")) education = "本科"
      else if (education.contains("硕士") || education.contains("研究生") ||
        education.contains("MPA") || education.contains("MBA") || education.contains("EMBA")) education = "硕士"
      else if (education.contains("博士")) education = "博士"
      else education = "本科"

      var isCQ = row.getAs("city").toString().contains("重庆")

      if (categorySet.&(yydzjsSet).size > 0) {
      }
      if (categorySet.&(jsjyyjsSet).size > 0) {
      }

    }

    // 专业需求趋势
    val yydzjsEdu = yydzjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jsjyyjsEdu = jsjyyjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val rjjsEdu = rjjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val yjsjsyyyEdu = yjsjsyyyMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val ydyykfEdu = ydyykfMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val wlwyyjsEdu = wlwyyjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val txjsEdu = txjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jzzsgcjsEdu = jzzsgcjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jzsnsjEdu = jzsnsjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jzgcjsEdu = jzgcjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jsgcglEdu = jsgcglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val gczjEdu = gczjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val fdcjyyglEdu = fdcjyyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val csgdjtgcjsEdu = csgdjtgcjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val skjsEdu = skjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jzdqgcjsEdu = jzdqgcjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jdythjsEdu = jdythjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val dqzdhjsEdu = dqzdhjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jdsbwxyglEdu = jdsbwxyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val gyjqrjsEdu = gyjqrjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val qcjcywxjsEdu = qcjcywxjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val qczzyzpjsEdu = qczzyzpjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val xnyqcjsEdu = xnyqcjsMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val qcyxyfwEdu = qcyxyfwMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val zqyqhEdu = zqyqhMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val kjEdu = kjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val scyxEdu = scyxMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val gjswEdu = gjswMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val dzswEdu = dzswMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val swyyEdu = swyyMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val wlglEdu = wlglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val gsqyglEdu = gsqyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val lsjyglEdu = lsjyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val lyglEdu = lyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jdglEdu = jdglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val hzchyglEdu = hzchyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val whscjyglEdu = whscjyglMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val gbysjmzzEdu = gbysjmzzMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val ysdhEdu = ysdhMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val hjyssjEdu = hjyssjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val cpyssjEdu = cpyssjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val fzyfssjEdu = fzyfssjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val sjcbsjyzzEdu = sjcbsjyzzMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val szmtyssjEdu = szmtyssjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val dmsjEdu = dmsjMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    val jzdhymszzEdu = jzdhymszzMap.toSeq.sortWith(_._2>_._2).mkString(";").replace("(","").replace(")", "").trim
    
    val majorEdu = MajorEdu(yesterday, yydzjsEdu, jsjyyjsEdu, rjjsEdu, yjsjsyyyEdu, ydyykfEdu, wlwyyjsEdu, txjsEdu, jzzsgcjsEdu, jzsnsjEdu, jzgcjsEdu, jsgcglEdu, gczjEdu, 
        fdcjyyglEdu, csgdjtgcjsEdu, skjsEdu, jzdqgcjsEdu, jdythjsEdu, dqzdhjsEdu, jdsbwxyglEdu, gyjqrjsEdu, qcjcywxjsEdu, qczzyzpjsEdu, xnyqcjsEdu, qcyxyfwEdu, zqyqhEdu, 
        kjEdu, scyxEdu, gjswEdu, dzswEdu, swyyEdu, wlglEdu, gsqyglEdu, lsjyglEdu, lyglEdu, jdglEdu, hzchyglEdu, whscjyglEdu, gbysjmzzEdu, ysdhEdu, hjyssjEdu, 
        cpyssjEdu, fzyfssjEdu, sjcbsjyzzEdu, szmtyssjEdu, dmsjEdu, jzdhymszzEdu)
    val majorEduList = List(majorEdu)

    val yydzjsEduCQ = yydzjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jsjyyjsEduCQ = jsjyyjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val rjjsEduCQ = rjjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val yjsjsyyyEduCQ = yjsjsyyyMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val ydyykfEduCQ = ydyykfMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val wlwyyjsEduCQ = wlwyyjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val txjsEduCQ = txjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jzzsgcjsEduCQ = jzzsgcjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jzsnsjEduCQ = jzsnsjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jzgcjsEduCQ = jzgcjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jsgcglEduCQ = jsgcglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val gczjEduCQ = gczjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val fdcjyyglEduCQ = fdcjyyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val csgdjtgcjsEduCQ = csgdjtgcjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val skjsEduCQ = skjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jzdqgcjsEduCQ = jzdqgcjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jdythjsEduCQ = jdythjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val dqzdhjsEduCQ = dqzdhjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jdsbwxyglEduCQ = jdsbwxyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val gyjqrjsEduCQ = gyjqrjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val qcjcywxjsEduCQ = qcjcywxjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val qczzyzpjsEduCQ = qczzyzpjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val xnyqcjsEduCQ = xnyqcjsMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val qcyxyfwEduCQ = qcyxyfwMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val zqyqhEduCQ = zqyqhMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val kjEduCQ = kjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val scyxEduCQ = scyxMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val gjswEduCQ = gjswMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val dzswEduCQ = dzswMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val swyyEduCQ = swyyMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val wlglEduCQ = wlglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val gsqyglEduCQ = gsqyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val lsjyglEduCQ = lsjyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val lyglEduCQ = lyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jdglEduCQ = jdglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val hzchyglEduCQ = hzchyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val whscjyglEduCQ = whscjyglMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val gbysjmzzEduCQ = gbysjmzzMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val ysdhEduCQ = ysdhMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val hjyssjEduCQ = hjyssjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val cpyssjEduCQ = cpyssjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val fzyfssjEduCQ = fzyfssjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val sjcbsjyzzEduCQ = sjcbsjyzzMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val szmtyssjEduCQ = szmtyssjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val dmsjEduCQ = dmsjMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim
    val jzdhymszzEduCQ = jzdhymszzMapCQ.toSeq.sortWith(_._2 > _._2).mkString(";").replace("(", "").replace(")", "").trim

    val majorEduCQ = MajorEdu(yesterday, yydzjsEduCQ, jsjyyjsEduCQ, rjjsEduCQ, yjsjsyyyEduCQ, ydyykfEduCQ, wlwyyjsEduCQ, txjsEduCQ, jzzsgcjsEduCQ, jzsnsjEduCQ, jzgcjsEduCQ,
      jsgcglEduCQ, gczjEduCQ, fdcjyyglEduCQ, csgdjtgcjsEduCQ, skjsEduCQ, jzdqgcjsEduCQ, jdythjsEduCQ, dqzdhjsEduCQ, jdsbwxyglEduCQ, gyjqrjsEduCQ, qcjcywxjsEduCQ,
      qczzyzpjsEduCQ, xnyqcjsEduCQ, qcyxyfwEduCQ, zqyqhEduCQ, kjEduCQ, scyxEduCQ, gjswEduCQ, dzswEduCQ, swyyEduCQ, wlglEduCQ, gsqyglEduCQ, lsjyglEduCQ, lyglEduCQ,
      jdglEduCQ, hzchyglEduCQ, whscjyglEduCQ, gbysjmzzEduCQ, ysdhEduCQ, hjyssjEduCQ, cpyssjEduCQ, fzyfssjEduCQ, sjcbsjyzzEduCQ, szmtyssjEduCQ, dmsjEduCQ, jzdhymszzEduCQ)
    val majorEduListCQ = List(majorEduCQ)

    val majorEduSchema = StructType(
      List(
        StructField("trendDate", StringType, true),
        StructField("yydzjs_edu", StringType, true),
        StructField("jsjyyjs_edu", StringType, true),
        StructField("rjjs_edu", StringType, true),
        StructField("yjsjsyyy_edu", StringType, true),
        StructField("ydyykf_edu", StringType, true),
        StructField("wlwyyjs_edu", StringType, true),
        StructField("txjs_edu", StringType, true),
        StructField("jzzsgcjs_edu", StringType, true),
        StructField("jzsnsj_edu", StringType, true),
        StructField("jzgcjs_edu", StringType, true),
        StructField("jsgcgl_edu", StringType, true),
        StructField("gczj_edu", StringType, true),
        StructField("fdcjyygl_edu", StringType, true),
        StructField("csgdjtgcjs_edu", StringType, true),
        StructField("skjs_edu", StringType, true),
        StructField("jzdqgcjs_edu", StringType, true),
        StructField("jdythjs_edu", StringType, true),
        StructField("dqzdhjs_edu", StringType, true),
        StructField("jdsbwxygl_edu", StringType, true),
        StructField("gyjqrjs_edu", StringType, true),
        StructField("qcjcywxjs_edu", StringType, true),
        StructField("qczzyzpjs_edu", StringType, true),
        StructField("xnyqcjs_edu", StringType, true),
        StructField("qcyxyfw_edu", StringType, true),
        StructField("zqyqh_edu", StringType, true),
        StructField("kj_edu", StringType, true),
        StructField("scyx_edu", StringType, true),
        StructField("gjsw_edu", StringType, true),
        StructField("dzsw_edu", StringType, true),
        StructField("swyy_edu", StringType, true),
        StructField("wlgl_edu", StringType, true),
        StructField("gsqygl_edu", StringType, true),
        StructField("lsjygl_edu", StringType, true),
        StructField("lygl_edu", StringType, true),
        StructField("jdgl_edu", StringType, true),
        StructField("hzchygl_edu", StringType, true),
        StructField("whscjygl_edu", StringType, true),
        StructField("gbysjmzz_edu", StringType, true),
        StructField("ysdh_edu", StringType, true),
        StructField("hjyssj_edu", StringType, true),
        StructField("cpyssj_edu", StringType, true),
        StructField("fzyfssj_edu", StringType, true),
        StructField("sjcbsjyzz_edu", StringType, true),
        StructField("szmtyssj_edu", StringType, true),
        StructField("dmsj_edu", StringType, true),
        StructField("jzdhymszz_edu", StringType, true)
      )
    )

    var majorEduRDD = ss.sparkContext.parallelize(majorEduList.distinct)
    var rowRDD = majorEduRDD.map(majorEdu => Row(majorEdu.trendDate, majorEdu.yydzjsEdu, majorEdu.jsjyyjsEdu, majorEdu.rjjsEdu, majorEdu.yjsjsyyyEdu,
      majorEdu.ydyykfEdu, majorEdu.wlwyyjsEdu, majorEdu.txjsEdu, majorEdu.jzzsgcjsEdu, majorEdu.jzsnsjEdu, majorEdu.jzgcjsEdu, majorEdu.jsgcglEdu,
      majorEdu.gczjEdu, majorEdu.fdcjyyglEdu, majorEdu.csgdjtgcjsEdu, majorEdu.skjsEdu, majorEdu.jzdqgcjsEdu, majorEdu.jdythjsEdu, majorEdu.dqzdhjsEdu,
      majorEdu.jdsbwxyglEdu, majorEdu.gyjqrjsEdu, majorEdu.qcjcywxjsEdu, majorEdu.qczzyzpjsEdu, majorEdu.xnyqcjsEdu, majorEdu.qcyxyfwEdu,
      majorEdu.zqyqhEdu, majorEdu.kjEdu, majorEdu.scyxEdu, majorEdu.gjswEdu, majorEdu.dzswEdu, majorEdu.swyyEdu, majorEdu.wlglEdu, majorEdu.gsqyglEdu,
      majorEdu.lsjyglEdu, majorEdu.lyglEdu, majorEdu.jdglEdu, majorEdu.hzchyglEdu, majorEdu.whscjyglEdu, majorEdu.gbysjmzzEdu, majorEdu.ysdhEdu,
      majorEdu.hjyssjEdu, majorEdu.cpyssjEdu, majorEdu.fzyfssjEdu, majorEdu.sjcbsjyzzEdu, majorEdu.szmtyssjEdu, majorEdu.dmsjEdu, majorEdu.jzdhymszzEdu))
    val majorEduDataFrame = ss.sqlContext.createDataFrame(rowRDD, majorEduSchema)

    majorEduDataFrame.write.mode("append").jdbc(mysqlUrl, "bigdata_edutrendday", mysqlProps)

    var majorEduRDDCQ = ss.sparkContext.parallelize(majorEduListCQ.distinct)
    var rowRDDCQ = majorEduRDDCQ.map(majorEdu => Row(majorEdu.trendDate, majorEdu.yydzjsEdu, majorEdu.jsjyyjsEdu, majorEdu.rjjsEdu, majorEdu.yjsjsyyyEdu,
      majorEdu.ydyykfEdu, majorEdu.wlwyyjsEdu, majorEdu.txjsEdu, majorEdu.jzzsgcjsEdu, majorEdu.jzsnsjEdu, majorEdu.jzgcjsEdu, majorEdu.jsgcglEdu,
      majorEdu.gczjEdu, majorEdu.fdcjyyglEdu, majorEdu.csgdjtgcjsEdu, majorEdu.skjsEdu, majorEdu.jzdqgcjsEdu, majorEdu.jdythjsEdu, majorEdu.dqzdhjsEdu,
      majorEdu.jdsbwxyglEdu, majorEdu.gyjqrjsEdu, majorEdu.qcjcywxjsEdu, majorEdu.qczzyzpjsEdu, majorEdu.xnyqcjsEdu, majorEdu.qcyxyfwEdu,
      majorEdu.zqyqhEdu, majorEdu.kjEdu, majorEdu.scyxEdu, majorEdu.gjswEdu, majorEdu.dzswEdu, majorEdu.swyyEdu, majorEdu.wlglEdu, majorEdu.gsqyglEdu,
      majorEdu.lsjyglEdu, majorEdu.lyglEdu, majorEdu.jdglEdu, majorEdu.hzchyglEdu, majorEdu.whscjyglEdu, majorEdu.gbysjmzzEdu, majorEdu.ysdhEdu,
      majorEdu.hjyssjEdu, majorEdu.cpyssjEdu, majorEdu.fzyfssjEdu, majorEdu.sjcbsjyzzEdu, majorEdu.szmtyssjEdu, majorEdu.dmsjEdu, majorEdu.jzdhymszzEdu))
    val majorEduDataFrameCQ = ss.sqlContext.createDataFrame(rowRDDCQ, majorEduSchema)

    majorEduDataFrameCQ.write.mode("append").jdbc(mysqlUrl, "bigdata_edutrenddaycq", mysqlProps)
    
  }

  def eduPlusLoop(edu:String, eduMap:Map[String, Int]) = {
    val dazhuan = "大专"
    val benke = "本科"
    val shuoshi = "硕士"
    val boshi = "博士"

//    if (edu == dazhuan) {
//      if (eduMap.contains(benke)) eduMap += (benke -> (eduMap(benke) + 1))
//      else eduMap += (benke -> 1)
//
//      if (eduMap.contains(shuoshi)) eduMap += (shuoshi -> (eduMap(shuoshi) + 1))
//      else eduMap += (shuoshi -> 1)
//
//      if (eduMap.contains(boshi)) eduMap += (boshi -> (eduMap(boshi) + 1))
//      else eduMap += (boshi -> 1)
//    }
//    else if (edu == benke) {
//      if (eduMap.contains(shuoshi)) eduMap += (shuoshi -> (eduMap(shuoshi) + 1))
//      else eduMap += (shuoshi -> 1)
//
//      if (eduMap.contains(boshi)) eduMap += (boshi -> (eduMap(boshi) + 1))
//      else eduMap += (boshi -> 1)
//    }
//    else if (edu == shuoshi) {
//      if (eduMap.contains(boshi)) eduMap += (boshi -> (eduMap(boshi) + 1))
//      else eduMap += (boshi -> 1)
//    }
  }

  case class MajorEdu (
                        trendDate: String,
                        yydzjsEdu: String, // 应用电子技术
                        jsjyyjsEdu: String, // 网络技术 <- 计算机应用技术
                        rjjsEdu: String, // 软件技术
                        yjsjsyyyEdu: String, // 云计算技术与应用
                        ydyykfEdu: String, // 移动应用开发
                        wlwyyjsEdu: String, // 物联网应用技术
                        txjsEdu: String, // 通信技术
                        jzzsgcjsEdu: String, // 建筑装饰工程技术
                        jzsnsjEdu: String, // 建筑室内设计
                        jzgcjsEdu: String, // 建筑工程技术
                        jsgcglEdu: String, // 建设工程管理
                        gczjEdu: String, // 工程造价
                        fdcjyyglEdu: String, // 房地产经营与管理
                        csgdjtgcjsEdu: String, // 城市轨道交通工程技术
                        skjsEdu: String, // 数控技术
                        jzdqgcjsEdu: String, // 建筑电气工程技术
                        jdythjsEdu: String, // 机电一体化技术
                        dqzdhjsEdu: String, // 电气自动化技术
                        jdsbwxyglEdu: String, // 机电设备维修与管理
                        gyjqrjsEdu: String, // 工业机器人技术
                        qcjcywxjsEdu: String, // 汽车检测与维修技术
                        qczzyzpjsEdu: String, // 汽车制造与装配技术
                        xnyqcjsEdu: String, // 新能源汽车技术
                        qcyxyfwEdu: String, // 汽车营销与服务
                        zqyqhEdu: String, // 证券与期货
                        kjEdu: String, // 会计
                        scyxEdu: String, // 市场营销
                        gjswEdu: String, // 国际商务
                        dzswEdu: String, // 电子商务
                        swyyEdu: String, // 商务英语
                        wlglEdu: String, // 物流管理
                        gsqyglEdu: String, // 工商企业管理
                        lsjyglEdu: String, // 连锁经营管理
                        lyglEdu: String, // 旅游管理
                        jdglEdu: String, // 酒店管理
                        hzchyglEdu: String, // 会展策划与管理
                        whscjyglEdu: String, // 文化市场经营管理
                        gbysjmzzEdu: String, // 广播影视节目制作
                        ysdhEdu: String, // 影视动画
                        hjyssjEdu: String, // 环境艺术设计
                        cpyssjEdu: String, // 产品艺术设计
                        fzyfssjEdu: String, // 服装与服饰设计
                        sjcbsjyzzEdu: String, // 视觉传播设计与制作
                        szmtyssjEdu: String, // 数字媒体艺术设计
                        dmsjEdu: String, // 动漫设计
                        jzdhymszzEdu: String // 建筑动画与模式制作
                      )
  
}



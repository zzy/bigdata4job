package bd

import java.util.Properties
import org.apache.spark.sql.{ SparkSession, Row, Column}
import org.apache.spark.sql.types._

object Recmd {

  def main(args: Array[String]) {
    val ss = SparkSession.builder()
      .master("spark://隐藏")
      .appName("隐藏")
      .config("spark.executor.memory", "8g")
      .getOrCreate()

    val mysqlUrl = "jdbc:mysql://隐藏?useSSL=false"
    val mysqlProps = new Properties
    mysqlProps.setProperty("user", "root")
    mysqlProps.setProperty("password", "隐藏")

    val utils = new Utils

    //    获取推荐设定信息
    val majorDF = ss.read.jdbc(mysqlUrl, "bigdata_majorskill", mysqlProps)
    //    val majorIdColumn = new Column("majorSkillId")

    // yydzjs 应用电子技术 
    val majorYydzjsDF = majorDF.filter("majorSkillId=2")
    val majorYydzjsRow = majorYydzjsDF.first()
    val majorYydzjsKeywords = majorYydzjsRow.getString(2).split(",").toList
    val majorYydzjsSalary = majorYydzjsRow.getString(3).split("-")
    var salaryMinYydzjs = majorYydzjsSalary(0).toInt
    var salaryMaxYydzjs = majorYydzjsSalary(1).toInt
    val majorYydzjsCity = majorYydzjsRow.getString(4).split(",").toList
    val majorYydzjsEducation = majorYydzjsRow.getString(5).split(",").toList

    //  jsjyyjs 计算机应用技术
    val majorJsjyyjsDF = majorDF.filter("majorSkillId=3")
    val majorJsjyyjsRow = majorJsjyyjsDF.first()
    val majorJsjyyjsKeywords = majorJsjyyjsRow.getString(2).split(",").toList
    val majorJsjyyjsSalary = majorJsjyyjsRow.getString(3).split("-")
    var salaryMinJsjyyjs = majorJsjyyjsSalary(0).toInt
    var salaryMaxJsjyyjs = majorJsjyyjsSalary(1).toInt
    val majorJsjyyjsCity = majorJsjyyjsRow.getString(4).split(",").toList
    val majorJsjyyjsEducation = majorJsjyyjsRow.getString(5).split(",").toList

    // rjjs  软件技术
    val majorRjjsDF = majorDF.filter("majorSkillId=4")
    val majorRjjsRow = majorRjjsDF.first()
    val majorRjjsKeywords = majorRjjsRow.getString(2).split(",").toList
    val majorRjjsSalary = majorRjjsRow.getString(3).split("-")
    var salaryMinRjjs = majorRjjsSalary(0).toInt
    var salaryMaxRjjs = majorRjjsSalary(1).toInt
    val majorRjjsCity = majorRjjsRow.getString(4).split(",").toList
    val majorRjjsEducation = majorRjjsRow.getString(5).split(",").toList

    // yjsjsyyy  云计算技术与应用
    val majorYjsjsyyyDF = majorDF.filter("majorSkillId=5")
    val majorYjsjsyyyRow = majorYjsjsyyyDF.first()
    val majorYjsjsyyyKeywords = majorYjsjsyyyRow.getString(2).split(",").toList
    val majorYjsjsyyySalary = majorYjsjsyyyRow.getString(3).split("-")
    var salaryMinYjsjsyyy = majorYjsjsyyySalary(0).toInt
    var salaryMaxYjsjsyyy = majorYjsjsyyySalary(1).toInt
    val majorYjsjsyyyCity = majorYjsjsyyyRow.getString(4).split(",").toList
    val majorYjsjsyyyEducation = majorYjsjsyyyRow.getString(5).split(",").toList

    //  ydyykf  移动应用开发
    val majorYdyykfDF = majorDF.filter("majorSkillId=6")
    val majorYdyykfRow = majorYdyykfDF.first()
    val majorYdyykfKeywords = majorYdyykfRow.getString(2).split(",").toList
    val majorYdyykfSalary = majorYdyykfRow.getString(3).split("-")
    var salaryMinYdyykf = majorYdyykfSalary(0).toInt
    var salaryMaxYdyykf = majorYdyykfSalary(1).toInt
    val majorYdyykfCity = majorYdyykfRow.getString(4).split(",").toList
    val majorYdyykfEducation = majorYdyykfRow.getString(5).split(",").toList

    // wlwyyjs 物联网应用技术
    val majorWlwyyjsDF = majorDF.filter("majorSkillId=7")
    val majorWlwyyjsRow = majorWlwyyjsDF.first()
    val majorWlwyyjsKeywords = majorWlwyyjsRow.getString(2).split(",").toList
    val majorWlwyyjsSalary = majorWlwyyjsRow.getString(3).split("-")
    var salaryMinWlwyyjs = majorWlwyyjsSalary(0).toInt
    var salaryMaxWlwyyjs = majorWlwyyjsSalary(1).toInt
    val majorWlwyyjsCity = majorWlwyyjsRow.getString(4).split(",").toList
    val majorWlwyyjsEducation = majorWlwyyjsRow.getString(5).split(",").toList

    // txjs 通信技术
    val majorTxjsDF = majorDF.filter("majorSkillId=8")
    val majorTxjsRow = majorTxjsDF.first()
    val majorTxjsKeywords = majorTxjsRow.getString(2).split(",").toList
    val majorTxjsSalary = majorTxjsRow.getString(3).split("-")
    var salaryMinTxjs = majorTxjsSalary(0).toInt
    var salaryMaxTxjs = majorTxjsSalary(1).toInt
    val majorTxjsCity = majorTxjsRow.getString(4).split(",").toList
    val majorTxjsEducation = majorTxjsRow.getString(5).split(",").toList

    // jzzsgcjs 建筑装饰工程技术
    val majorJzzsgcjsDF = majorDF.filter("majorSkillId=9")
    val majorJzzsgcjsRow = majorJzzsgcjsDF.first()
    val majorJzzsgcjsKeywords = majorJzzsgcjsRow.getString(2).split(",").toList
    val majorJzzsgcjsSalary = majorJzzsgcjsRow.getString(3).split("-")
    var salaryMinJzzsgcjs = majorJzzsgcjsSalary(0).toInt
    var salaryMaxJzzsgcjs = majorJzzsgcjsSalary(1).toInt
    val majorJzzsgcjsCity = majorJzzsgcjsRow.getString(4).split(",").toList
    val majorJzzsgcjsEducation = majorJzzsgcjsRow.getString(5).split(",").toList

    // jzsnsj  建筑室内设计
    val majorJzsnsjDF = majorDF.filter("majorSkillId=10")
    val majorJzsnsjRow = majorJzsnsjDF.first()
    val majorJzsnsjKeywords = majorJzsnsjRow.getString(2).split(",").toList
    val majorJzsnsjSalary = majorJzsnsjRow.getString(3).split("-")
    var salaryMinJzsnsj = majorJzsnsjSalary(0).toInt
    var salaryMaxJzsnsj = majorJzsnsjSalary(1).toInt
    val majorJzsnsjCity = majorJzsnsjRow.getString(4).split(",").toList
    val majorJzsnsjEducation = majorJzsnsjRow.getString(5).split(",").toList

    // jzgcjs  建筑工程技术
    val majorJzgcjsDF = majorDF.filter("majorSkillId=11")
    val majorJzgcjsRow = majorJzgcjsDF.first()
    val majorJzgcjsKeywords = majorJzgcjsRow.getString(2).split(",").toList
    val majorJzgcjsSalary = majorJzgcjsRow.getString(3).split("-")
    var salaryMinJzgcjs = majorJzgcjsSalary(0).toInt
    var salaryMaxJzgcjs = majorJzgcjsSalary(1).toInt
    val majorJzgcjsCity = majorJzgcjsRow.getString(4).split(",").toList
    val majorJzgcjsEducation = majorJzgcjsRow.getString(5).split(",").toList

    // jsgcgl 建设工程管理
    val majorJsgcglDF = majorDF.filter("majorSkillId=12")
    val majorJsgcglRow = majorJsgcglDF.first()
    val majorJsgcglKeywords = majorJsgcglRow.getString(2).split(",").toList
    val majorJsgcglSalary = majorJsgcglRow.getString(3).split("-")
    var salaryMinJsgcgl = majorJsgcglSalary(0).toInt
    var salaryMaxJsgcgl = majorJsgcglSalary(1).toInt
    val majorJsgcglCity = majorJsgcglRow.getString(4).split(",").toList
    val majorJsgcglEducation = majorJsgcglRow.getString(5).split(",").toList

    // gczj 工程造价
    val majorGczjDF = majorDF.filter("majorSkillId=13")
    val majorGczjRow = majorGczjDF.first()
    val majorGczjKeywords = majorGczjRow.getString(2).split(",").toList
    val majorGczjSalary = majorGczjRow.getString(3).split("-")
    var salaryMinGczj = majorGczjSalary(0).toInt
    var salaryMaxGczj = majorGczjSalary(1).toInt
    val majorGczjCity = majorGczjRow.getString(4).split(",").toList
    val majorGczjEducation = majorGczjRow.getString(5).split(",").toList

    // fdcjyygl 房地产经营与管理
    val majorFdcjyyglDF = majorDF.filter("majorSkillId=14")
    val majorFdcjyyglRow = majorFdcjyyglDF.first()
    val majorFdcjyyglKeywords = majorFdcjyyglRow.getString(2).split(",").toList
    val majorFdcjyyglSalary = majorFdcjyyglRow.getString(3).split("-")
    var salaryMinFdcjyygl = majorFdcjyyglSalary(0).toInt
    var salaryMaxFdcjyygl = majorFdcjyyglSalary(1).toInt
    val majorFdcjyyglCity = majorFdcjyyglRow.getString(4).split(",").toList
    val majorFdcjyyglEducation = majorFdcjyyglRow.getString(5).split(",").toList

    // csgdjtgcjs  城市轨道交通工程技术
    val majorCsgdjtgcjsDF = majorDF.filter("majorSkillId=15")
    val majorCsgdjtgcjsRow = majorCsgdjtgcjsDF.first()
    val majorCsgdjtgcjsKeywords = majorCsgdjtgcjsRow.getString(2).split(",").toList
    val majorCsgdjtgcjsSalary = majorCsgdjtgcjsRow.getString(3).split("-")
    var salaryMinCsgdjtgcjs = majorCsgdjtgcjsSalary(0).toInt
    var salaryMaxCsgdjtgcjs = majorCsgdjtgcjsSalary(1).toInt
    val majorCsgdjtgcjsCity = majorCsgdjtgcjsRow.getString(4).split(",").toList
    val majorCsgdjtgcjsEducation = majorCsgdjtgcjsRow.getString(5).split(",").toList

    // skjs 数控技术
    val majorSkjsDF = majorDF.filter("majorSkillId=16")
    val majorSkjsRow = majorSkjsDF.first()
    val majorSkjsKeywords = majorSkjsRow.getString(2).split(",").toList
    val majorSkjsSalary = majorSkjsRow.getString(3).split("-")
    var salaryMinSkjs = majorSkjsSalary(0).toInt
    var salaryMaxSkjs = majorSkjsSalary(1).toInt
    val majorSkjsCity = majorSkjsRow.getString(4).split(",").toList
    val majorSkjsEducation = majorSkjsRow.getString(5).split(",").toList

    // jzdqgcjs 建筑电气工程技术
    val majorJzdqgcjsDF = majorDF.filter("majorSkillId=17")
    val majorJzdqgcjsRow = majorJzdqgcjsDF.first()
    val majorJzdqgcjsKeywords = majorJzdqgcjsRow.getString(2).split(",").toList
    val majorJzdqgcjsSalary = majorJzdqgcjsRow.getString(3).split("-")
    var salaryMinJzdqgcjs = majorJzdqgcjsSalary(0).toInt
    var salaryMaxJzdqgcjs = majorJzdqgcjsSalary(1).toInt
    val majorJzdqgcjsCity = majorJzdqgcjsRow.getString(4).split(",").toList
    val majorJzdqgcjsEducation = majorJzdqgcjsRow.getString(5).split(",").toList

    // jdythjs 机电一体化技术
    val majorJdythjsDF = majorDF.filter("majorSkillId=18")
    val majorJdythjsRow = majorJdythjsDF.first()
    val majorJdythjsKeywords = majorJdythjsRow.getString(2).split(",").toList
    val majorJdythjsSalary = majorJdythjsRow.getString(3).split("-")
    var salaryMinJdythjs = majorJdythjsSalary(0).toInt
    var salaryMaxJdythjs = majorJdythjsSalary(1).toInt
    val majorJdythjsCity = majorJdythjsRow.getString(4).split(",").toList
    val majorJdythjsEducation = majorJdythjsRow.getString(5).split(",").toList

    // dqzdhjs 电气自动化技术
    val majorDqzdhjsDF = majorDF.filter("majorSkillId=19")
    val majorDqzdhjsRow = majorDqzdhjsDF.first()
    val majorDqzdhjsKeywords = majorDqzdhjsRow.getString(2).split(",").toList
    val majorDqzdhjsSalary = majorDqzdhjsRow.getString(3).split("-")
    var salaryMinDqzdhjs = majorDqzdhjsSalary(0).toInt
    var salaryMaxDqzdhjs = majorDqzdhjsSalary(1).toInt
    val majorDqzdhjsCity = majorDqzdhjsRow.getString(4).split(",").toList
    val majorDqzdhjsEducation = majorDqzdhjsRow.getString(5).split(",").toList

    // jdsbwxygl 机电设备维修与管理
    val majorJdsbwxyglDF = majorDF.filter("majorSkillId=20")
    val majorJdsbwxyglRow = majorJdsbwxyglDF.first()
    val majorJdsbwxyglKeywords = majorJdsbwxyglRow.getString(2).split(",").toList
    val majorJdsbwxyglSalary = majorJdsbwxyglRow.getString(3).split("-")
    var salaryMinJdsbwxygl = majorJdsbwxyglSalary(0).toInt
    var salaryMaxJdsbwxygl = majorJdsbwxyglSalary(1).toInt
    val majorJdsbwxyglCity = majorJdsbwxyglRow.getString(4).split(",").toList
    val majorJdsbwxyglEducation = majorJdsbwxyglRow.getString(5).split(",").toList

    // gyjqrjs 工业机器人技术
    val majorGyjqrjsDF = majorDF.filter("majorSkillId=21")
    val majorGyjqrjsRow = majorGyjqrjsDF.first()
    val majorGyjqrjsKeywords = majorGyjqrjsRow.getString(2).split(",").toList
    val majorGyjqrjsSalary = majorGyjqrjsRow.getString(3).split("-")
    var salaryMinGyjqrjs = majorGyjqrjsSalary(0).toInt
    var salaryMaxGyjqrjs = majorGyjqrjsSalary(1).toInt
    val majorGyjqrjsCity = majorGyjqrjsRow.getString(4).split(",").toList
    val majorGyjqrjsEducation = majorGyjqrjsRow.getString(5).split(",").toList

    // qcjcywxjs 汽车检测与维修技术
    val majorQcjcywxjsDF = majorDF.filter("majorSkillId=22")
    val majorQcjcywxjsRow = majorQcjcywxjsDF.first()
    val majorQcjcywxjsKeywords = majorQcjcywxjsRow.getString(2).split(",").toList
    val majorQcjcywxjsSalary = majorQcjcywxjsRow.getString(3).split("-")
    var salaryMinQcjcywxjs = majorQcjcywxjsSalary(0).toInt
    var salaryMaxQcjcywxjs = majorQcjcywxjsSalary(1).toInt
    val majorQcjcywxjsCity = majorQcjcywxjsRow.getString(4).split(",").toList
    val majorQcjcywxjsEducation = majorQcjcywxjsRow.getString(5).split(",").toList

    // qczzyzpjs 汽车制造与装配技术
    val majorQczzyzpjsDF = majorDF.filter("majorSkillId=23")
    val majorQczzyzpjsRow = majorQczzyzpjsDF.first()
    val majorQczzyzpjsKeywords = majorQczzyzpjsRow.getString(2).split(",").toList
    val majorQczzyzpjsSalary = majorQczzyzpjsRow.getString(3).split("-")
    var salaryMinQczzyzpjs = majorQczzyzpjsSalary(0).toInt
    var salaryMaxQczzyzpjs = majorQczzyzpjsSalary(1).toInt
    val majorQczzyzpjsCity = majorQczzyzpjsRow.getString(4).split(",").toList
    val majorQczzyzpjsEducation = majorQczzyzpjsRow.getString(5).split(",").toList

    // xnyqcjs 新能源汽车技术
    val majorXnyqcjsDF = majorDF.filter("majorSkillId=24")
    val majorXnyqcjsRow = majorXnyqcjsDF.first()
    val majorXnyqcjsKeywords = majorXnyqcjsRow.getString(2).split(",").toList
    val majorXnyqcjsSalary = majorXnyqcjsRow.getString(3).split("-")
    var salaryMinXnyqcjs = majorXnyqcjsSalary(0).toInt
    var salaryMaxXnyqcjs = majorXnyqcjsSalary(1).toInt
    val majorXnyqcjsCity = majorXnyqcjsRow.getString(4).split(",").toList
    val majorXnyqcjsEducation = majorXnyqcjsRow.getString(5).split(",").toList

    // qcyxyfw 汽车营销与服务
    val majorQcyxyfwDF = majorDF.filter("majorSkillId=25")
    val majorQcyxyfwRow = majorQcyxyfwDF.first()
    val majorQcyxyfwKeywords = majorQcyxyfwRow.getString(2).split(",").toList
    val majorQcyxyfwSalary = majorQcyxyfwRow.getString(3).split("-")
    var salaryMinQcyxyfw = majorQcyxyfwSalary(0).toInt
    var salaryMaxQcyxyfw = majorQcyxyfwSalary(1).toInt
    val majorQcyxyfwCity = majorQcyxyfwRow.getString(4).split(",").toList
    val majorQcyxyfwEducation = majorQcyxyfwRow.getString(5).split(",").toList

    // zqyqh 证券与期货
    val majorZqyqhDF = majorDF.filter("majorSkillId=26")
    val majorZqyqhRow = majorZqyqhDF.first()
    val majorZqyqhKeywords = majorZqyqhRow.getString(2).split(",").toList
    val majorZqyqhSalary = majorZqyqhRow.getString(3).split("-")
    var salaryMinZqyqh = majorZqyqhSalary(0).toInt
    var salaryMaxZqyqh = majorZqyqhSalary(1).toInt
    val majorZqyqhCity = majorZqyqhRow.getString(4).split(",").toList
    val majorZqyqhEducation = majorZqyqhRow.getString(5).split(",").toList

    // kj 会计
    val majorKjDF = majorDF.filter("majorSkillId=27")
    val majorKjRow = majorKjDF.first()
    val majorKjKeywords = majorKjRow.getString(2).split(",").toList
    val majorKjSalary = majorKjRow.getString(3).split("-")
    var salaryMinKj = majorKjSalary(0).toInt
    var salaryMaxKj = majorKjSalary(1).toInt
    val majorKjCity = majorKjRow.getString(4).split(",").toList
    val majorKjEducation = majorKjRow.getString(5).split(",").toList

    // scyx 市场营销
    val majorScyxDF = majorDF.filter("majorSkillId=28")
    val majorScyxRow = majorScyxDF.first()
    val majorScyxKeywords = majorScyxRow.getString(2).split(",").toList
    val majorScyxSalary = majorScyxRow.getString(3).split("-")
    var salaryMinScyx = majorScyxSalary(0).toInt
    var salaryMaxScyx = majorScyxSalary(1).toInt
    val majorScyxCity = majorScyxRow.getString(4).split(",").toList
    val majorScyxEducation = majorScyxRow.getString(5).split(",").toList

    // gjsw 国际商务
    val majorGjswDF = majorDF.filter("majorSkillId=29")
    val majorGjswRow = majorGjswDF.first()
    val majorGjswKeywords = majorGjswRow.getString(2).split(",").toList
    val majorGjswSalary = majorGjswRow.getString(3).split("-")
    var salaryMinGjsw = majorGjswSalary(0).toInt
    var salaryMaxGjsw = majorGjswSalary(1).toInt
    val majorGjswCity = majorGjswRow.getString(4).split(",").toList
    val majorGjswEducation = majorGjswRow.getString(5).split(",").toList

    // dzsw 电子商务
    val majorDzswDF = majorDF.filter("majorSkillId=30")
    val majorDzswRow = majorDzswDF.first()
    val majorDzswKeywords = majorDzswRow.getString(2).split(",").toList
    val majorDzswSalary = majorDzswRow.getString(3).split("-")
    var salaryMinDzsw = majorDzswSalary(0).toInt
    var salaryMaxDzsw = majorDzswSalary(1).toInt
    val majorDzswCity = majorDzswRow.getString(4).split(",").toList
    val majorDzswEducation = majorDzswRow.getString(5).split(",").toList

    // swyy 商务英语
    val majorSwyyDF = majorDF.filter("majorSkillId=31")
    val majorSwyyRow = majorSwyyDF.first()
    val majorSwyyKeywords = majorSwyyRow.getString(2).split(",").toList
    val majorSwyySalary = majorSwyyRow.getString(3).split("-")
    var salaryMinSwyy = majorSwyySalary(0).toInt
    var salaryMaxSwyy = majorSwyySalary(1).toInt
    val majorSwyyCity = majorSwyyRow.getString(4).split(",").toList
    val majorSwyyEducation = majorSwyyRow.getString(5).split(",").toList

    // wlgl 物流管理
    val majorWlglDF = majorDF.filter("majorSkillId=32")
    val majorWlglRow = majorWlglDF.first()
    val majorWlglKeywords = majorWlglRow.getString(2).split(",").toList
    val majorWlglSalary = majorWlglRow.getString(3).split("-")
    var salaryMinWlgl = majorWlglSalary(0).toInt
    var salaryMaxWlgl = majorWlglSalary(1).toInt
    val majorWlglCity = majorWlglRow.getString(4).split(",").toList
    val majorWlglEducation = majorWlglRow.getString(5).split(",").toList

    // gsqygl 工商企业管理
    val majorGsqyglDF = majorDF.filter("majorSkillId=33")
    val majorGsqyglRow = majorGsqyglDF.first()
    val majorGsqyglKeywords = majorGsqyglRow.getString(2).split(",").toList
    val majorGsqyglSalary = majorGsqyglRow.getString(3).split("-")
    var salaryMinGsqygl = majorGsqyglSalary(0).toInt
    var salaryMaxGsqygl = majorGsqyglSalary(1).toInt
    val majorGsqyglCity = majorGsqyglRow.getString(4).split(",").toList
    val majorGsqyglEducation = majorGsqyglRow.getString(5).split(",").toList

    // lsjygl 连锁经营管理
    val majorLsjyglDF = majorDF.filter("majorSkillId=34")
    val majorLsjyglRow = majorLsjyglDF.first()
    val majorLsjyglKeywords = majorLsjyglRow.getString(2).split(",").toList
    val majorLsjyglSalary = majorLsjyglRow.getString(3).split("-")
    var salaryMinLsjygl = majorLsjyglSalary(0).toInt
    var salaryMaxLsjygl = majorLsjyglSalary(1).toInt
    val majorLsjyglCity = majorLsjyglRow.getString(4).split(",").toList
    val majorLsjyglEducation = majorLsjyglRow.getString(5).split(",").toList

    // lygl 旅游管理
    val majorLyglDF = majorDF.filter("majorSkillId=35")
    val majorLyglRow = majorLyglDF.first()
    val majorLyglKeywords = majorLyglRow.getString(2).split(",").toList
    val majorLyglSalary = majorLyglRow.getString(3).split("-")
    var salaryMinLygl = majorLyglSalary(0).toInt
    var salaryMaxLygl = majorLyglSalary(1).toInt
    val majorLyglCity = majorLyglRow.getString(4).split(",").toList
    val majorLyglEducation = majorLyglRow.getString(5).split(",").toList

    // jdgl 酒店管理
    val majorJdglDF = majorDF.filter("majorSkillId=36")
    val majorJdglRow = majorJdglDF.first()
    val majorJdglKeywords = majorJdglRow.getString(2).split(",").toList
    val majorJdglSalary = majorJdglRow.getString(3).split("-")
    var salaryMinJdgl = majorJdglSalary(0).toInt
    var salaryMaxJdgl = majorJdglSalary(1).toInt
    val majorJdglCity = majorJdglRow.getString(4).split(",").toList
    val majorJdglEducation = majorJdglRow.getString(5).split(",").toList

    // hzchygl 会展策划与管理
    val majorHzchyglDF = majorDF.filter("majorSkillId=37")
    val majorHzchyglRow = majorHzchyglDF.first()
    val majorHzchyglKeywords = majorHzchyglRow.getString(2).split(",").toList
    val majorHzchyglSalary = majorHzchyglRow.getString(3).split("-")
    var salaryMinHzchygl = majorHzchyglSalary(0).toInt
    var salaryMaxHzchygl = majorHzchyglSalary(1).toInt
    val majorHzchyglCity = majorHzchyglRow.getString(4).split(",").toList
    val majorHzchyglEducation = majorHzchyglRow.getString(5).split(",").toList

    // whscjygl 文化市场经营管理
    val majorWhscjyglDF = majorDF.filter("majorSkillId=38")
    val majorWhscjyglRow = majorWhscjyglDF.first()
    val majorWhscjyglKeywords = majorWhscjyglRow.getString(2).split(",").toList
    val majorWhscjyglSalary = majorWhscjyglRow.getString(3).split("-")
    var salaryMinWhscjygl = majorWhscjyglSalary(0).toInt
    var salaryMaxWhscjygl = majorWhscjyglSalary(1).toInt
    val majorWhscjyglCity = majorWhscjyglRow.getString(4).split(",").toList
    val majorWhscjyglEducation = majorWhscjyglRow.getString(5).split(",").toList

    // gbysjmzz 广播影视节目制作
    val majorGbysjmzzDF = majorDF.filter("majorSkillId=39")
    val majorGbysjmzzRow = majorGbysjmzzDF.first()
    val majorGbysjmzzKeywords = majorGbysjmzzRow.getString(2).split(",").toList
    val majorGbysjmzzSalary = majorGbysjmzzRow.getString(3).split("-")
    var salaryMinGbysjmzz = majorGbysjmzzSalary(0).toInt
    var salaryMaxGbysjmzz = majorGbysjmzzSalary(1).toInt
    val majorGbysjmzzCity = majorGbysjmzzRow.getString(4).split(",").toList
    val majorGbysjmzzEducation = majorGbysjmzzRow.getString(5).split(",").toList

    // ysdh 影视动画
    val majorYsdhDF = majorDF.filter("majorSkillId=40")
    val majorYsdhRow = majorYsdhDF.first()
    val majorYsdhKeywords = majorYsdhRow.getString(2).split(",").toList
    val majorYsdhSalary = majorYsdhRow.getString(3).split("-")
    var salaryMinYsdh = majorYsdhSalary(0).toInt
    var salaryMaxYsdh = majorYsdhSalary(1).toInt
    val majorYsdhCity = majorYsdhRow.getString(4).split(",").toList
    val majorYsdhEducation = majorYsdhRow.getString(5).split(",").toList

    // hjyssj 环境艺术设计
    val majorHjyssjDF = majorDF.filter("majorSkillId=41")
    val majorHjyssjRow = majorHjyssjDF.first()
    val majorHjyssjKeywords = majorHjyssjRow.getString(2).split(",").toList
    val majorHjyssjSalary = majorHjyssjRow.getString(3).split("-")
    var salaryMinHjyssj = majorHjyssjSalary(0).toInt
    var salaryMaxHjyssj = majorHjyssjSalary(1).toInt
    val majorHjyssjCity = majorHjyssjRow.getString(4).split(",").toList
    val majorHjyssjEducation = majorHjyssjRow.getString(5).split(",").toList

    // cpyssj 产品艺术设计
    val majorCpyssjDF = majorDF.filter("majorSkillId=42")
    val majorCpyssjRow = majorCpyssjDF.first()
    val majorCpyssjKeywords = majorCpyssjRow.getString(2).split(",").toList
    val majorCpyssjSalary = majorCpyssjRow.getString(3).split("-")
    var salaryMinCpyssj = majorCpyssjSalary(0).toInt
    var salaryMaxCpyssj = majorCpyssjSalary(1).toInt
    val majorCpyssjCity = majorCpyssjRow.getString(4).split(",").toList
    val majorCpyssjEducation = majorCpyssjRow.getString(5).split(",").toList

    // fzyfssj 服装与服饰设计
    val majorFzyfssjDF = majorDF.filter("majorSkillId=43")
    val majorFzyfssjRow = majorFzyfssjDF.first()
    val majorFzyfssjKeywords = majorFzyfssjRow.getString(2).split(",").toList
    val majorFzyfssjSalary = majorFzyfssjRow.getString(3).split("-")
    var salaryMinFzyfssj = majorFzyfssjSalary(0).toInt
    var salaryMaxFzyfssj = majorFzyfssjSalary(1).toInt
    val majorFzyfssjCity = majorFzyfssjRow.getString(4).split(",").toList
    val majorFzyfssjEducation = majorFzyfssjRow.getString(5).split(",").toList

    // sjcbsjyzz 视觉传播设计与制作
    val majorSjcbsjyzzDF = majorDF.filter("majorSkillId=44")
    val majorSjcbsjyzzRow = majorSjcbsjyzzDF.first()
    val majorSjcbsjyzzKeywords = majorSjcbsjyzzRow.getString(2).split(",").toList
    val majorSjcbsjyzzSalary = majorSjcbsjyzzRow.getString(3).split("-")
    var salaryMinSjcbsjyzz = majorSjcbsjyzzSalary(0).toInt
    var salaryMaxSjcbsjyzz = majorSjcbsjyzzSalary(1).toInt
    val majorSjcbsjyzzCity = majorSjcbsjyzzRow.getString(4).split(",").toList
    val majorSjcbsjyzzEducation = majorSjcbsjyzzRow.getString(5).split(",").toList

    // szmtyssj 数字媒体艺术设计
    val majorSzmtyssjDF = majorDF.filter("majorSkillId=45")
    val majorSzmtyssjRow = majorSzmtyssjDF.first()
    val majorSzmtyssjKeywords = majorSzmtyssjRow.getString(2).split(",").toList
    val majorSzmtyssjSalary = majorSzmtyssjRow.getString(3).split("-")
    var salaryMinSzmtyssj = majorSzmtyssjSalary(0).toInt
    var salaryMaxSzmtyssj = majorSzmtyssjSalary(1).toInt
    val majorSzmtyssjCity = majorSzmtyssjRow.getString(4).split(",").toList
    val majorSzmtyssjEducation = majorSzmtyssjRow.getString(5).split(",").toList

    // dmsj 动漫设计
    val majorDmsjDF = majorDF.filter("majorSkillId=46")
    val majorDmsjRow = majorDmsjDF.first()
    val majorDmsjKeywords = majorDmsjRow.getString(2).split(",").toList
    val majorDmsjSalary = majorDmsjRow.getString(3).split("-")
    var salaryMinDmsj = majorDmsjSalary(0).toInt
    var salaryMaxDmsj = majorDmsjSalary(1).toInt
    val majorDmsjCity = majorDmsjRow.getString(4).split(",").toList
    val majorDmsjEducation = majorDmsjRow.getString(5).split(",").toList

    // jzdhymszz 建筑动画与模式制作
    val majorJzdhymszzDF = majorDF.filter("majorSkillId=47")
    val majorJzdhymszzRow = majorJzdhymszzDF.first()
    val majorJzdhymszzKeywords = majorJzdhymszzRow.getString(2).split(",").toList
    val majorJzdhymszzSalary = majorJzdhymszzRow.getString(3).split("-")
    var salaryMinJzdhymszz = majorJzdhymszzSalary(0).toInt
    var salaryMaxJzdhymszz = majorJzdhymszzSalary(1).toInt
    val majorJzdhymszzCity = majorJzdhymszzRow.getString(4).split(",").toList
    val majorJzdhymszzEducation = majorJzdhymszzRow.getString(5).split(",").toList

    //  获取职位数据
    var recommendDay = utils.getYesterday
    // if (utils.isAfternoon) recommendDay = utils.getToday
    
    val salaryMinColumn = new Column("salary_min")
    val salaryMaxColumn = new Column("salary_max")
    val experienceColumn = new Column("experience")
    //  val dayDF = ss.read.jdbc(mysqlUrl, "bigdata_job", Array("date_='2017-11-13'"), mysqlProps)
    val dayDF = ss.read.jdbc(mysqlUrl, "bigdata_job", Array("date_='" + recommendDay + "'"), mysqlProps)
    println(dayDF.count())

    val recommendDF = dayDF.filter(salaryMinColumn >= salaryMinRjjs && salaryMaxColumn <= salaryMaxRjjs
      && (experienceColumn.contains("无工作经验") || experienceColumn.contains("无经验")
        || experienceColumn.contains("在读") || experienceColumn.contains("应届")
        || experienceColumn.contains("1年以下")))
    println(recommendDF.count())

    var recommendList: List[Job] = List[Job]()

    //    判断职位信息是否符合推荐设定
    for (row <- recommendDF.collect()) {
      var title = row.getAs("title").toString()
      var jobCompany = row.getAs("job_company").toString()

      var city = row.getAs("city").toString()
      var education = row.getAs("education").toString()

      var isTitleFilter = titleFilter(title)
      var isCompanyFilter = companyFilter(jobCompany)
      if (!isTitleFilter && !isCompanyFilter) {
        var reBatch = utils.batchNumber
        var salaryMin = row.getAs("salary_min").toString().toInt
        var salaryMax = row.getAs("salary_max").toString().toInt
        var area = row.getAs("area").toString()
        var catalog = row.getAs("catalog").toString()
        var category = row.getAs("category").toString()
        var experience = row.getAs("experience").toString()
        var jobDesc = row.getAs("job_desc").toString()
        var jobUrl = row.getAs("job_url").toString()
        var fromSite = row.getAs("from_site").toString()
        var reDate = utils.getToday
        var majorId = 0

        // yydzjs 应用电子技术
        var isYydzjsKeywordsFit = containStr(title, majorYydzjsKeywords)
        var isYydzjsCityFit = containStr(city, majorYydzjsCity)
        var isYydzjsEduFit = (education.contains("无要求") || containStr(education, majorYydzjsEducation))
        if (isYydzjsKeywordsFit && isYydzjsCityFit && isYydzjsEduFit) {
          majorId = 2

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        //  jsjyyjs 计算机应用技术
        var isJsjyyjsKeywordsFit = containStr(title, majorJsjyyjsKeywords)
        var isJsjyyjsCityFit = containStr(city, majorJsjyyjsCity)
        var isJsjyyjsEduFit = (education.contains("无要求") || containStr(education, majorJsjyyjsEducation))
        if (isJsjyyjsKeywordsFit && isJsjyyjsCityFit && isJsjyyjsEduFit) {
          majorId = 3

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // rjjs  软件技术
        var isRjjsKeywordsFit = containStr(title, majorRjjsKeywords)
        var isRjjsCityFit = containStr(city, majorRjjsCity)
        var isRjjsEduFit = (education.contains("无要求") || containStr(education, majorRjjsEducation))
        if (isRjjsKeywordsFit && isRjjsCityFit && isRjjsEduFit) {
          majorId = 4

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // yjsjsyyy  云计算技术与应用
        var isYjsjsyyyKeywordsFit = containStr(title, majorYjsjsyyyKeywords)
        var isYjsjsyyyCityFit = containStr(city, majorYjsjsyyyCity)
        var isYjsjsyyyEduFit = (education.contains("无要求") || containStr(education, majorYjsjsyyyEducation))
        if (isYjsjsyyyKeywordsFit && isYjsjsyyyCityFit && isYjsjsyyyEduFit) {
          majorId = 5

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        //  ydyykf  移动应用开发
        var isYdyykfKeywordsFit = containStr(title, majorYdyykfKeywords)
        var isYdyykfCityFit = containStr(city, majorYdyykfCity)
        var isYdyykfEduFit = (education.contains("无要求") || containStr(education, majorYdyykfEducation))
        if (isYdyykfKeywordsFit && isYdyykfCityFit && isYdyykfEduFit) {
          majorId = 6

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // wlwyyjs 物联网应用技术
        var isWlwyyjsKeywordsFit = containStr(title, majorWlwyyjsKeywords)
        var isWlwyyjsCityFit = containStr(city, majorWlwyyjsCity)
        var isWlwyyjsEduFit = (education.contains("无要求") || containStr(education, majorWlwyyjsEducation))
        if (isWlwyyjsKeywordsFit && isWlwyyjsCityFit && isWlwyyjsEduFit) {
          majorId = 7

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // txjs 通信技术
        var isTxjsKeywordsFit = containStr(title, majorTxjsKeywords)
        var isTxjsCityFit = containStr(city, majorTxjsCity)
        var isTxjsEduFit = (education.contains("无要求") || containStr(education, majorTxjsEducation))
        if (isTxjsKeywordsFit && isTxjsCityFit && isTxjsEduFit) {
          majorId = 8

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jzzsgcjs 建筑装饰工程技术
        var isJzzsgcjsKeywordsFit = containStr(title, majorJzzsgcjsKeywords)
        var isJzzsgcjsCityFit = containStr(city, majorJzzsgcjsCity)
        var isJzzsgcjsEduFit = (education.contains("无要求") || containStr(education, majorJzzsgcjsEducation))
        if (isJzzsgcjsKeywordsFit && isJzzsgcjsCityFit && isJzzsgcjsEduFit) {
          majorId = 9

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jzsnsj  建筑室内设计
        var isJzsnsjKeywordsFit = containStr(title, majorJzsnsjKeywords)
        var isJzsnsjCityFit = containStr(city, majorJzsnsjCity)
        var isJzsnsjEduFit = (education.contains("无要求") || containStr(education, majorJzsnsjEducation))
        if (isJzsnsjKeywordsFit && isJzsnsjCityFit && isJzsnsjEduFit) {
          majorId = 10

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jzgcjs  建筑工程技术
        var isJzgcjsKeywordsFit = containStr(title, majorJzgcjsKeywords)
        var isJzgcjsCityFit = containStr(city, majorJzgcjsCity)
        var isJzgcjsEduFit = (education.contains("无要求") || containStr(education, majorJzgcjsEducation))
        if (isJzgcjsKeywordsFit && isJzgcjsCityFit && isJzgcjsEduFit) {
          majorId = 11

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jsgcgl 建设工程管理
        var isJsgcglKeywordsFit = containStr(title, majorJsgcglKeywords)
        var isJsgcglCityFit = containStr(city, majorJsgcglCity)
        var isJsgcglEduFit = (education.contains("无要求") || containStr(education, majorJsgcglEducation))
        if (isJsgcglKeywordsFit && isJsgcglCityFit && isJsgcglEduFit) {
          majorId = 12

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // gczj 工程造价
        var isGczjKeywordsFit = containStr(title, majorGczjKeywords)
        var isGczjCityFit = containStr(city, majorGczjCity)
        var isGczjEduFit = (education.contains("无要求") || containStr(education, majorGczjEducation))
        if (isGczjKeywordsFit && isGczjCityFit && isGczjEduFit) {
          majorId = 13

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // fdcjyygl 房地产经营与管理
        var isFdcjyyglKeywordsFit = containStr(title, majorFdcjyyglKeywords)
        var isFdcjyyglCityFit = containStr(city, majorFdcjyyglCity)
        var isFdcjyyglEduFit = (education.contains("无要求") || containStr(education, majorFdcjyyglEducation))
        if (isFdcjyyglKeywordsFit && isFdcjyyglCityFit && isFdcjyyglEduFit) {
          majorId = 14

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // csgdjtgcjs  城市轨道交通工程技术
        var isCsgdjtgcjsKeywordsFit = containStr(title, majorCsgdjtgcjsKeywords)
        var isCsgdjtgcjsCityFit = containStr(city, majorCsgdjtgcjsCity)
        var isCsgdjtgcjsEduFit = (education.contains("无要求") || containStr(education, majorCsgdjtgcjsEducation))
        if (isCsgdjtgcjsKeywordsFit && isCsgdjtgcjsCityFit && isCsgdjtgcjsEduFit) {
          majorId = 15

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // skjs 数控技术
        var isSkjsKeywordsFit = containStr(title, majorSkjsKeywords)
        var isSkjsCityFit = containStr(city, majorSkjsCity)
        var isSkjsEduFit = (education.contains("无要求") || containStr(education, majorSkjsEducation))
        if (isSkjsKeywordsFit && isSkjsCityFit && isSkjsEduFit) {
          majorId = 16

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jzdqgcjs 建筑电气工程技术
        var isJzdqgcjsKeywordsFit = containStr(title, majorJzdqgcjsKeywords)
        var isJzdqgcjsCityFit = containStr(city, majorJzdqgcjsCity)
        var isJzdqgcjsEduFit = (education.contains("无要求") || containStr(education, majorJzdqgcjsEducation))
        if (isJzdqgcjsKeywordsFit && isJzdqgcjsCityFit && isJzdqgcjsEduFit) {
          majorId = 17

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jdythjs 机电一体化技术
        var isJdythjsKeywordsFit = containStr(title, majorJdythjsKeywords)
        var isJdythjsCityFit = containStr(city, majorJdythjsCity)
        var isJdythjsEduFit = (education.contains("无要求") || containStr(education, majorJdythjsEducation))
        if (isJdythjsKeywordsFit && isJdythjsCityFit && isJdythjsEduFit) {
          majorId = 18

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // dqzdhjs 电气自动化技术
        var isDqzdhjsKeywordsFit = containStr(title, majorDqzdhjsKeywords)
        var isDqzdhjsCityFit = containStr(city, majorDqzdhjsCity)
        var isDqzdhjsEduFit = (education.contains("无要求") || containStr(education, majorDqzdhjsEducation))
        if (isDqzdhjsKeywordsFit && isDqzdhjsCityFit && isDqzdhjsEduFit) {
          majorId = 19

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jdsbwxygl 机电设备维修与管理
        var isJdsbwxyglKeywordsFit = containStr(title, majorJdsbwxyglKeywords)
        var isJdsbwxyglCityFit = containStr(city, majorJdsbwxyglCity)
        var isJdsbwxyglEduFit = (education.contains("无要求") || containStr(education, majorJdsbwxyglEducation))
        if (isJdsbwxyglKeywordsFit && isJdsbwxyglCityFit && isJdsbwxyglEduFit) {
          majorId = 20

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // gyjqrjs 工业机器人技术
        var isGyjqrjsKeywordsFit = containStr(title, majorGyjqrjsKeywords)
        var isGyjqrjsCityFit = containStr(city, majorGyjqrjsCity)
        var isGyjqrjsEduFit = (education.contains("无要求") || containStr(education, majorGyjqrjsEducation))
        if (isGyjqrjsKeywordsFit && isGyjqrjsCityFit && isGyjqrjsEduFit) {
          majorId = 21

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // qcjcywxjs 汽车检测与维修技术
        var isQcjcywxjsKeywordsFit = containStr(title, majorQcjcywxjsKeywords)
        var isQcjcywxjsCityFit = containStr(city, majorQcjcywxjsCity)
        var isQcjcywxjsEduFit = (education.contains("无要求") || containStr(education, majorQcjcywxjsEducation))
        if (isQcjcywxjsKeywordsFit && isQcjcywxjsCityFit && isQcjcywxjsEduFit) {
          majorId = 22

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // qczzyzpjs 汽车制造与装配技术
        var isQczzyzpjsKeywordsFit = containStr(title, majorQczzyzpjsKeywords)
        var isQczzyzpjsCityFit = containStr(city, majorQczzyzpjsCity)
        var isQczzyzpjsEduFit = (education.contains("无要求") || containStr(education, majorQczzyzpjsEducation))
        if (isQczzyzpjsKeywordsFit && isQczzyzpjsCityFit && isQczzyzpjsEduFit) {
          majorId = 23

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // xnyqcjs 新能源汽车技术
        var isXnyqcjsKeywordsFit = containStr(title, majorXnyqcjsKeywords)
        var isXnyqcjsCityFit = containStr(city, majorXnyqcjsCity)
        var isXnyqcjsEduFit = (education.contains("无要求") || containStr(education, majorXnyqcjsEducation))
        if (isXnyqcjsKeywordsFit && isXnyqcjsCityFit && isXnyqcjsEduFit) {
          majorId = 24

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // qcyxyfw 汽车营销与服务
        var isQcyxyfwKeywordsFit = containStr(title, majorQcyxyfwKeywords)
        var isQcyxyfwCityFit = containStr(city, majorQcyxyfwCity)
        var isQcyxyfwEduFit = (education.contains("无要求") || containStr(education, majorQcyxyfwEducation))
        if (isQcyxyfwKeywordsFit && isQcyxyfwCityFit && isQcyxyfwEduFit) {
          majorId = 25

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // zqyqh 证券与期货
        var isZqyqhKeywordsFit = containStr(title, majorZqyqhKeywords)
        var isZqyqhCityFit = containStr(city, majorZqyqhCity)
        var isZqyqhEduFit = (education.contains("无要求") || containStr(education, majorZqyqhEducation))
        if (isZqyqhKeywordsFit && isZqyqhCityFit && isZqyqhEduFit) {
          majorId = 26

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // kj 会计
        var isKjKeywordsFit = containStr(title, majorKjKeywords)
        var isKjCityFit = containStr(city, majorKjCity)
        var isKjEduFit = (education.contains("无要求") || containStr(education, majorKjEducation))
        if (isKjKeywordsFit && isKjCityFit && isKjEduFit) {
          majorId = 27

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // scyx 市场营销
        var isScyxKeywordsFit = containStr(title, majorScyxKeywords)
        var isScyxCityFit = containStr(city, majorScyxCity)
        var isScyxEduFit = (education.contains("无要求") || containStr(education, majorScyxEducation))
        if (isScyxKeywordsFit && isScyxCityFit && isScyxEduFit) {
          majorId = 28

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // gjsw 国际商务
        var isGjswKeywordsFit = containStr(title, majorGjswKeywords)
        var isGjswCityFit = containStr(city, majorGjswCity)
        var isGjswEduFit = (education.contains("无要求") || containStr(education, majorGjswEducation))
        if (isGjswKeywordsFit && isGjswCityFit && isGjswEduFit) {
          majorId = 29

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // dzsw 电子商务
        var isDzswKeywordsFit = containStr(title, majorDzswKeywords)
        var isDzswCityFit = containStr(city, majorDzswCity)
        var isDzswEduFit = (education.contains("无要求") || containStr(education, majorDzswEducation))
        if (isDzswKeywordsFit && isDzswCityFit && isDzswEduFit) {
          majorId = 30

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // swyy 商务英语
        var isSwyyKeywordsFit = containStr(title, majorSwyyKeywords)
        var isSwyyCityFit = containStr(city, majorSwyyCity)
        var isSwyyEduFit = (education.contains("无要求") || containStr(education, majorSwyyEducation))
        if (isSwyyKeywordsFit && isSwyyCityFit && isSwyyEduFit) {
          majorId = 31

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // wlgl 物流管理
        var isWlglKeywordsFit = containStr(title, majorWlglKeywords)
        var isWlglCityFit = containStr(city, majorWlglCity)
        var isWlglEduFit = (education.contains("无要求") || containStr(education, majorWlglEducation))
        if (isWlglKeywordsFit && isWlglCityFit && isWlglEduFit) {
          majorId = 32

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // gsqygl 工商企业管理
        var isGsqyglKeywordsFit = containStr(title, majorGsqyglKeywords)
        var isGsqyglCityFit = containStr(city, majorGsqyglCity)
        var isGsqyglEduFit = (education.contains("无要求") || containStr(education, majorGsqyglEducation))
        if (isGsqyglKeywordsFit && isGsqyglCityFit && isGsqyglEduFit) {
          majorId = 33

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // lsjygl 连锁经营管理
        var isLsjyglKeywordsFit = containStr(title, majorLsjyglKeywords)
        var isLsjyglCityFit = containStr(city, majorLsjyglCity)
        var isLsjyglEduFit = (education.contains("无要求") || containStr(education, majorLsjyglEducation))
        if (isLsjyglKeywordsFit && isLsjyglCityFit && isLsjyglEduFit) {
          majorId = 34

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // lygl 旅游管理
        var isLyglKeywordsFit = containStr(title, majorLyglKeywords)
        var isLyglCityFit = containStr(city, majorLyglCity)
        var isLyglEduFit = (education.contains("无要求") || containStr(education, majorLyglEducation))
        if (isLyglKeywordsFit && isLyglCityFit && isLyglEduFit) {
          majorId = 35

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jdgl 酒店管理
        var isJdglKeywordsFit = containStr(title, majorJdglKeywords)
        var isJdglCityFit = containStr(city, majorJdglCity)
        var isJdglEduFit = (education.contains("无要求") || containStr(education, majorJdglEducation))
        if (isJdglKeywordsFit && isJdglCityFit && isJdglEduFit) {
          majorId = 36

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // hzchygl 会展策划与管理
        var isHzchyglKeywordsFit = containStr(title, majorHzchyglKeywords)
        var isHzchyglCityFit = containStr(city, majorHzchyglCity)
        var isHzchyglEduFit = (education.contains("无要求") || containStr(education, majorHzchyglEducation))
        if (isHzchyglKeywordsFit && isHzchyglCityFit && isHzchyglEduFit) {
          majorId = 37

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // whscjygl 文化市场经营管理
        var isWhscjyglKeywordsFit = containStr(title, majorWhscjyglKeywords)
        var isWhscjyglCityFit = containStr(city, majorWhscjyglCity)
        var isWhscjyglEduFit = (education.contains("无要求") || containStr(education, majorWhscjyglEducation))
        if (isWhscjyglKeywordsFit && isWhscjyglCityFit && isWhscjyglEduFit) {
          majorId = 38

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // gbysjmzz 广播影视节目制作
        var isGbysjmzzKeywordsFit = containStr(title, majorGbysjmzzKeywords)
        var isGbysjmzzCityFit = containStr(city, majorGbysjmzzCity)
        var isGbysjmzzEduFit = (education.contains("无要求") || containStr(education, majorGbysjmzzEducation))
        if (isGbysjmzzKeywordsFit && isGbysjmzzCityFit && isGbysjmzzEduFit) {
          majorId = 39

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // ysdh 影视动画
        var isYsdhKeywordsFit = containStr(title, majorYsdhKeywords)
        var isYsdhCityFit = containStr(city, majorYsdhCity)
        var isYsdhEduFit = (education.contains("无要求") || containStr(education, majorYsdhEducation))
        if (isYsdhKeywordsFit && isYsdhCityFit && isYsdhEduFit) {
          majorId = 40

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // hjyssj 环境艺术设计
        var isHjyssjKeywordsFit = containStr(title, majorHjyssjKeywords)
        var isHjyssjCityFit = containStr(city, majorHjyssjCity)
        var isHjyssjEduFit = (education.contains("无要求") || containStr(education, majorHjyssjEducation))
        if (isHjyssjKeywordsFit && isHjyssjCityFit && isHjyssjEduFit) {
          majorId = 41

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // cpyssj 产品艺术设计
        var isCpyssjKeywordsFit = containStr(title, majorCpyssjKeywords)
        var isCpyssjCityFit = containStr(city, majorCpyssjCity)
        var isCpyssjEduFit = (education.contains("无要求") || containStr(education, majorCpyssjEducation))
        if (isCpyssjKeywordsFit && isCpyssjCityFit && isCpyssjEduFit) {
          majorId = 42

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // fzyfssj 服装与服饰设计
        var isFzyfssjKeywordsFit = containStr(title, majorFzyfssjKeywords)
        var isFzyfssjCityFit = containStr(city, majorFzyfssjCity)
        var isFzyfssjEduFit = (education.contains("无要求") || containStr(education, majorFzyfssjEducation))
        if (isFzyfssjKeywordsFit && isFzyfssjCityFit && isFzyfssjEduFit) {
          majorId = 43

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // sjcbsjyzz 视觉传播设计与制作
        var isSjcbsjyzzKeywordsFit = containStr(title, majorSjcbsjyzzKeywords)
        var isSjcbsjyzzCityFit = containStr(city, majorSjcbsjyzzCity)
        var isSjcbsjyzzEduFit = (education.contains("无要求") || containStr(education, majorSjcbsjyzzEducation))
        if (isSjcbsjyzzKeywordsFit && isSjcbsjyzzCityFit && isSjcbsjyzzEduFit) {
          majorId = 44

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // szmtyssj 数字媒体艺术设计
        var isSzmtyssjKeywordsFit = containStr(title, majorSzmtyssjKeywords)
        var isSzmtyssjCityFit = containStr(city, majorSzmtyssjCity)
        var isSzmtyssjEduFit = (education.contains("无要求") || containStr(education, majorSzmtyssjEducation))
        if (isSzmtyssjKeywordsFit && isSzmtyssjCityFit && isSzmtyssjEduFit) {
          majorId = 45

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // dmsj 动漫设计
        var isDmsjKeywordsFit = containStr(title, majorDmsjKeywords)
        var isDmsjCityFit = containStr(city, majorDmsjCity)
        var isDmsjEduFit = (education.contains("无要求") || containStr(education, majorDmsjEducation))
        if (isDmsjKeywordsFit && isDmsjCityFit && isDmsjEduFit) {
          majorId = 46

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

        // jzdhymszz 建筑动画与模式制作
        var isJzdhymszzKeywordsFit = containStr(title, majorJzdhymszzKeywords)
        var isJzdhymszzCityFit = containStr(city, majorJzdhymszzCity)
        var isJzdhymszzEduFit = (education.contains("无要求") || containStr(education, majorJzdhymszzEducation))
        if (isJzdhymszzKeywordsFit && isJzdhymszzCityFit && isJzdhymszzEduFit) {
          majorId = 47

          val job = Job(reBatch, title, salaryMin, salaryMax, city, area, catalog, category,
            experience, education, jobDesc, jobUrl, jobCompany, fromSite, reDate, majorId)
          recommendList ::= job
        }

      }
    }

    recommendList = recommendList.distinct
    println(recommendList.size)

    // 职位信息写入推荐表
    if (!recommendList.isEmpty) {
      val jobSchema = StructType(
        List(
          StructField("rebatch", StringType, true),
          StructField("title", StringType, true),
          StructField("salary_min", IntegerType, true),
          StructField("salary_max", IntegerType, true),
          StructField("city", StringType, true),
          StructField("area", StringType, true),
          StructField("catalog", StringType, true),
          StructField("category", StringType, true),
          StructField("experience", StringType, true),
          StructField("education", StringType, true),
          StructField("job_desc", StringType, true),
          StructField("job_url", StringType, true),
          StructField("job_company", StringType, true),
          StructField("from_site", StringType, true),
          StructField("date_", StringType, true),
          StructField("majorSkillId", IntegerType, true)
        )
      )

      var jobRDD = ss.sparkContext.parallelize(recommendList.distinct)
      var rowRDD = jobRDD.map(job => Row(job.reBatch, job.title, job.salary_min, job.salary_max, job.city, job.area,
        job.catalog, job.category, job.experience, job.education, job.jobDesc, job.jobUrl, job.jobCompany, job.fromSite, job.reDate, job.majorId))
      val recommendDataFrame = ss.sqlContext.createDataFrame(rowRDD, jobSchema)

      recommendDataFrame.write.mode("append").jdbc(mysqlUrl, "bigdata_rejob", mysqlProps)
    }

    

    // 公司推荐
    var companyList: List[Company] = List[Company]()
    
    var companyInfoList: List[(String, Int)] = List[(String, Int)]()
    for (job <- recommendList) {
      companyInfoList ::= (job.jobCompany.trim(), job.majorId)
    }

    for (job <- companyInfoList.distinct) {
      val companyDF = ss.read.jdbc(mysqlUrl, "bigdata_company", Array("name='" + job._1 + "'"), mysqlProps)
      
      if (companyDF.count() > 0) {
        val companyRow = companyDF.first()
        var reBatch = utils.batchNumber
        var name = companyRow.getAs("name").toString()
        var companySize = companyRow.getAs("company_size").toString()
        var companyType = companyRow.getAs("company_type").toString()
        var companyCategory = companyRow.getAs("compnay_category").toString()
        var companyProvince = companyRow.getAs("company_province").toString()
        var companyCity = companyRow.getAs("company_city").toString()
        var phoneNum = companyRow.getAs("phone_num").toString()
        var address = companyRow.getAs("address").toString()
        var companyDesc = companyRow.getAs("company_desc").toString()
        var companyUrl = companyRow.getAs("company_url").toString()
        var reDate = utils.getToday
        var majorId = job._2

        var company = Company(reBatch, name, companySize, companyType, companyCategory,
          companyProvince, companyCity, phoneNum, address, companyDesc, companyUrl, reDate, majorId)
        companyList ::= company
      }
    }

    companyList = companyList.distinct
    println(companyList.size)

    // 公司信息写入推荐表
    if (!companyList.isEmpty) {
      val companySchema = StructType(
        List(
          StructField("rebatch", StringType, true),
          StructField("name", StringType, true),
          StructField("company_size", StringType, true),
          StructField("company_type", StringType, true),
          StructField("compnay_category", StringType, true),
          StructField("company_province", StringType, true),
          StructField("company_city", StringType, true),
          StructField("phone_num", StringType, true),
          StructField("address", StringType, true),
          StructField("company_desc", StringType, true),
          StructField("company_url", StringType, true),
          StructField("date_", StringType, true),
          StructField("majorSkillId", IntegerType, true)
        )
      )

      var companyRDD = ss.sparkContext.parallelize(companyList.distinct)
      var companyRowRDD = companyRDD.map(company => Row(company.reBatch, company.name, company.companySize, company.companyType, company.companyCategory,
        company.companyProvince, company.companyCity, company.phoneNum, company.address, company.companyDesc, company.companyUrl, company.reDate, company.majorId))
      val recompanyDataFrame = ss.sqlContext.createDataFrame(companyRowRDD, companySchema)

      recompanyDataFrame.write.mode("append").jdbc(mysqlUrl, "bigdata_recompany", mysqlProps)
    }
    
  }

  def titleFilter(title: String): Boolean = {
    var filterList = List("零基础", "0基础", "转行", "培训", "实训", "实习", "培养", "学徒",
      "助理", "名企", "委培")

    var isFilter = false

    import scala.util.control._

    val loop = new Breaks;
    loop.breakable {
      for (filterStr <- filterList) {
        if (title.contains(filterStr)) {
          isFilter = true
          loop.break()
        }
      }
    }

    isFilter
  }

  def containStr(rawStr: String, strList: List[String]): Boolean = {
    var isContain = false

    import scala.util.control._
    val loop = new Breaks;
    loop.breakable {
      for (str <- strList) {
        val str2 = str.toLowerCase().trim()
        if (!"".equalsIgnoreCase(str2)) isContain = rawStr.toLowerCase().contains(str2)
        if (isContain) loop.break()
      }
    }

    isContain
  }

  def companyFilter(companyName: String): Boolean = {
    var filterList = List("教育", "培训", "实训", "培养", "创智", "汇智", "创想", "育道", 
      "达内", "传智播客", "易思哲", "中航云软", "和禹网络", "谢尔科技",
      "睿峰科技", "狮子座科技", "智游网络", "格睿泰思", "华信智原", "百年有为", "亿昇威")

    var isFilter = false

    import scala.util.control._

    val loop = new Breaks;
    loop.breakable {
      for (filterStr <- filterList) {
        if (companyName.contains(filterStr)) {
          isFilter = true
          loop.break()
        }
      }
    }

    isFilter
  }

  case class Major (
    majorSkillId: Int,
    majorName: String,
    jobKeywords: String,
    salaryScope: String,
    jobCity: String,
    qualifications: String
  )
  
  case class Job (
    reBatch: String,
    title: String,
    salary_min: Int,
    salary_max: Int,
    city: String,
    area: String,
    catalog: String,
    category: String,
    experience: String,
    education: String,
    jobDesc: String,
    jobUrl: String,
    jobCompany: String,
    fromSite: String,
    reDate: String,
    majorId: Int
  )

  case class Company (
    reBatch: String,
    name: String,
    companySize: String,
    companyType: String,
    companyCategory: String,
    companyProvince: String,
    companyCity: String,
    phoneNum: String,
    address: String,
    companyDesc: String,
    companyUrl: String,
    reDate: String,
    majorId: Int
  )

}



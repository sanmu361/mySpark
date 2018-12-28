package base

import scala.beans.BeanProperty

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-21 20:06
  **/
class GDP(avg_gdp1:Double,avg_per1:Double,id1:Int,region_name1:String) extends Serializable{

  @BeanProperty var avg_gdp:Double = avg_gdp1
  @BeanProperty var avg_per:Double = avg_per1
  @BeanProperty var id:Int = id1
  @BeanProperty var region_name:String = region_name1
}

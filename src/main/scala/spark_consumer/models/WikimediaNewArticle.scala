package spark_consumer.models

import java.sql.Date

case class WikimediaNewArticle(
                              domain:String,
                              uri:String,
                              pageTitle:String,
                              revLen:String,//Long,
                              revDate:String
                              )

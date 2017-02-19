import com.databricks.spark
import com.databricks.spark._
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object ChamberOfDeputies {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ChamberOfDeputies")
      .set("packages", "com.databricks:spark-xml_2.11:0.4.1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val filepath = new File(args(0))


    val filesAndCharsets = Map(
      "AnoAtual.xml" -> "utf-16le",
      "AnoAnterior.xml" -> "utf-16le",
      "AnosAnteriores.xml" -> "utf-16be"
    )

    val charset = filesAndCharsets.get(filepath.getName()) match {
      case Some(charset) => charset
      case None => "utf-8"
    }

    var dataset = sqlContext.read.format("com.databricks.spark.xml")
      .option("charset", charset)
      .option("excludeAttribute", true)
      .option("rowTag", "DESPESA")
      .load(filepath.getAbsolutePath())

    val column = "subquota_description"
    dataset = dataset
      .toDF(dataset.columns.map(col => col.toLowerCase):_*)
      .withColumnRenamed("idedocumento", "document_id")
      .withColumnRenamed("txnomeparlamentar", "congressperson_name")
      .withColumnRenamed("idecadastro", "congressperson_id")
      .withColumnRenamed("nucarteiraparlamentar", "congressperson_document")
      .withColumnRenamed("nulegislatura", "term")
      .withColumnRenamed("sguf", "state")
      .withColumnRenamed("sgpartido", "party")
      .withColumnRenamed("codlegislatura", "term_id")
      .withColumnRenamed("numsubcota", "subquota_number")
      .withColumnRenamed("txtdescricao", "subquota_description")
      .withColumnRenamed("numespecificacaosubcota", "subquota_group_id")
      .withColumnRenamed("txtdescricaoespecificacao", "subquota_group_description")
      .withColumnRenamed("txtfornecedor", "supplier")
      .withColumnRenamed("txtcnpjcpf", "cnpj_cpf")
      .withColumnRenamed("txtnumero", "document_number")
      .withColumnRenamed("indtipodocumento", "document_type")
      .withColumnRenamed("datemissao", "issue_date")
      .withColumnRenamed("vlrdocumento", "document_value")
      .withColumnRenamed("vlrglosa", "remark_value")
      .withColumnRenamed("vlrliquido", "net_value")
      .withColumnRenamed("nummes", "month")
      .withColumnRenamed("numano", "year")
      .withColumnRenamed("numparcela", "installment")
      .withColumnRenamed("txtpassageiro", "passenger")
      .withColumnRenamed("txttrecho", "leg_of_the_trip")
      .withColumnRenamed("numlote", "batch_number")
      .withColumnRenamed("numressarcimento", "reimbursement_number")
      .withColumnRenamed("vlrrestituicao", "reimbursement_value")
      .withColumnRenamed("nudeputadoid", "applicant_id")

    val totalNetValue = sum("net_value")
      .alias("total_net_value")
    val reimbursementValueTotal = sum("reimbursement_value")
      .alias("reimbursement_value_total")
    val reimbursementNumbers = collect_set("reimbursement_number")
      .alias("reimbursement_numbers")

    val aggregationKeys = List("year", "applicant_id", "document_id")
    val requiredValues = List("document_value", "reimbursement_number")
    dataset = dataset.na.drop(requiredValues ++ aggregationKeys)

    val aggregation = dataset.groupBy("year", "applicant_id", "document_id")
      .agg(totalNetValue, reimbursementValueTotal, reimbursementNumbers)

    dataset
      .dropDuplicates(aggregationKeys)
      .join(aggregation, aggregationKeys)
      .write.parquet(filepath.getParent() + "/reimbursements")

    sc.stop()
  }
}

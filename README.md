# Chamber of Deputies

```console
$ sbt package
$ spark-submit \
  --class "ChamberOfDeputies" \
  --packages com.databricks:spark-xml_2.11:0.4.1 \
  target/scala-2.11/chamber-of-deputies_2.11-1.0.jar ~/Code/serenata/serenata-de-amor/data/AnoAtual.xml
```

AnoAtual.xml - 3.3 MB - 48.25s user 2.83s system 369% cpu 13.830 total
AnoAnterior.xml - 18.8 MB - 540.25s user 9.39s system 663% cpu 1:22.85 total
AnosAnteriores.xml - 95.7 MB - 3597.09s user 59.21s system 701% cpu 8:41.32 total

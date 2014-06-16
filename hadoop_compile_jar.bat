del *.bak
del *.jar
rd classes /Q /S
mkdir classes
find src -name "*.java" > sources.list
javac -classpath E:\File\hadoop-1.2.1\hadoop-core-1.2.1.jar; @sources.list -d classes
jar -cvf KeyWordSegmentJob.jar -C bin/ .
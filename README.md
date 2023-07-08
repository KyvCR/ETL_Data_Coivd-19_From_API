# Batch Prosessing Data Covid-19
Projcet ini bertujuan untuk melihat perkembangan covid-19 saat ini, walaupun untuk di indonesia sendiri sudah menjadi endemi dari pandemi, akan tetapi harus tetap perlu waspada mengenai coivd-19 ini

Berdasarkan data yang ada, bahwa ternyata masih ada beberapa negara yang hingga saat ini masih terkena virus ini, dah bahkan masih ada yang menyebabkan kematian

maka dari itu project ini adalah untuk memonitor data covid tersebut, dengan melakuan batch prosessing setiap harinya, dengan menggunakan Ariflow dan metode ETL, lalu akan terbentuk sebuah ahkirnya akan terbentuk menjadi visualisai untuk memonitor data tersebut

# Sumber Data
data yang didapatkan pada bersumber dari website [covid-19.dataflowkit.com](https://covid-19.dataflowkit.com/), pada website tersebut mereka menyediakan data yang lengkap mengenai data covid-19  

![image](https://github.com/KyvCR/ELT_Coivd-19/assets/127940133/6f7f963e-72ed-4a7c-9ff8-69f24a290737)

# Airflow
pada ariflow saya membuat simpel DAG jadi mulai dari melakukan pengambilan data, Cleansing data kemudian lansung dimasukan kedalam database, kemudian baru melalukan Transformasi di dalam database 


![image](https://github.com/KyvCR/ELT_Coivd-19/assets/127940133/03319014-131b-4a90-8797-6d3101cd8434)

# Data pipeline
Gambara system data pipeline

![image](https://github.com/KyvCR/ELT_Coivd-19/assets/127940133/6f707789-176d-4469-8a84-dbd6b72ac2fe)





# Установка директории для считывание тестовых данных
setwd("inpute")

#Создание пустого датафрейма
data <- data.frame(callsign = character(),
                    firstseen = character(),
                    latitude_1 = numeric(),
                    stringsAsFactors = FALSE)

# Создание списка файлов CSV в директории
file_list <- list.files(pattern="*.csv")

# Чтение каждого файла из списка в R
for (file in file_list) {
  data_part <- read.csv(file)
  # Проведение необходимых операций с данными
  data_part <- data_part[, c("callsign", "firstseen", "latitude_1")]
  data <- rbind(data, data_part)
}

dups <- sum(duplicated(data))
data_duble <- head(data, 1336000)
df_test <- rbind(data, data_duble)
write.csv(data, file = "df_test.csv", row.names = FALSE)
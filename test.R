library(doParallel)
library(dplyr)
library(ggplot2)
# Выходим из папку inpute в корневую дирректорию
setwd("..")
#Считывание тестовых данных
data <- read.csv("df_test.csv")

# Создание параллельного кластера
cl <- makeCluster(detectCores())

# Регистрация кластера
registerDoParallel(cl)

# Удаление пустых строк / Удаление дубликатов в параллельном режиме / Преобразование строк в которых нет цифр в пустые /Удаление записей в промежутке от 1 до 3 часов ночи
df <- data %>% 
  distinct() %>%
  na.omit() %>% 
  mutate_at(vars(callsign), ~ifelse(grepl("\\d", .), ., "")) %>% 
  mutate(firstseen = as.POSIXct(firstseen)) %>%
  filter(!format(firstseen, "%H") %in% c("01", "02", "03"))

# Остановка кластера
stopCluster(cl)

# Группируем данные по часам и считаем количество уникальных занчений в столбце callsign, среднее и медиану для latitude_1 каждого часа
metrics_df <- df %>%
  group_by(hour = format(firstseen, "%H")) %>%
  summarise(unique_callsigns = n_distinct(callsign),
            mean_latitude_1 = mean(latitude_1),
            median_latitude_1 = median(latitude_1))
#Создает столбец для соединения с 
df$hour <- format(df$firstseen, "%H")
#Создаёт датафрейм с результатом merge отчищенных данных и результом расчётов
merged_df <- merge(df, metrics_df, by = "hour", all.x = TRUE)

#постороение гистограммы для данных
ggplot(df, aes(x = latitude_1)) + 
  geom_histogram(binwidth = 1, fill = "blue", color = "black") +
  labs(x = "Широта", y = "Количество") +
  ggtitle("Распределение широт")

# Рассчитываем доверительный интервал для среднего значения
ci <- t.test(df$latitude_1, conf.level = 0.95)$conf.int
# Выводим результат
cat("Доверительный интервал для среднего значения latitude_1: [", ci[1], ";", ci[2], "]", "\n")
#Создаем отдельный столбец с номером месяца
merged_df$month <- format(merged_df$firstseen, "%m")
# Рассчитываем средние значения для каждого месяца
df_means <- merged_df%>%
  group_by(month) %>%
  summarize(mean_latitude_1 = mean(latitude_1))

# Строим график среднего значения mean_latitude_1 колонки (y) по месяцам (x).
ggplot(merged_df, aes(x = format(firstseen, "%m"), y = mean_latitude_1)) + # x - месяц в формате MM, y - колонка y
  stat_summary(fun = mean, geom = "bar", fill = "blue", color = "black") + # среднее значение, столбик
  labs(x = "Месяц", y = "Среднее значение", title = "График среднего значения по месяцам")

# Разбиваем строки на отдельные символы и создаем вектор всех символов
chars <- unlist(strsplit(df$callsign, ""))
# Создаем таблицу частотности символов
freq_table <- table(chars)
# Создаем dataframe с данными для тепловой карты
df_heatmap <- data.frame(
  char = names(freq_table),
  freq = as.numeric(freq_table)
)
# Строим тепловую карту
ggplot(df_heatmap, aes(x = "", y = char, fill = freq)) +
  geom_tile() +
  scale_fill_gradient(low = "white", high = "blue") +
  ggtitle("Частота символов в колонке string")

# Первое дополнительное задание
# устанавливаем начальное значение генератора случайных чисел, чтобы результаты были воспроизводимыми
set.seed(123)

# определяем размер каждой части
n <- nrow(merged_df)
n1 <- round(n * 0.25)
n2 <- round(n * 0.5)

# создаем индекс для каждой части
idx1 <- sample(1:n, n1, replace = FALSE)
idx2 <- sample(setdiff(1:n, idx1), n1, replace = FALSE)
idx3 <- setdiff(1:n, union(idx1, idx2))

# разбиваем датафрейм на три части на основе индексов
part1 <- merged_df[idx1, ]
part2 <- merged_df[idx2, ]
part3 <- merged_df[idx3, ]
# Если p-value меньше 0.05, то мы можем считать различия статистически значимыми.

# проверяем статистическую значимость различий в колонке mean_latitude_1 в частях part1, part2 и part3
t.test(part1$mean_latitude_1, part2$mean_latitude_1)
t.test(part1$mean_latitude_1, part3$mean_latitude_1)
t.test(part2$mean_latitude_1, part3$mean_latitude_1)

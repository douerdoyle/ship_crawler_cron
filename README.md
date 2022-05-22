# ship_crawler_cron
使用 docker-compose，建立的可爬取寶船網、船訊網的 Container<br>

# 說明
docker-compose 檔案內有設定 Container 可使用的 CPU 與 Memory 上限<br>
本程式會去爬取與[船訊網](http://shipxy.com)的船隻資訊，只要在 MySQL Table 中設置好要爬取的區域，即可開始爬取所選區域內的船隻<br>

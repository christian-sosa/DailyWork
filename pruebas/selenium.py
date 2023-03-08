# test
from selenium import webdriver

# Inicializar el navegador web
driver = webdriver.Chrome()

# Navegar a la página web
driver.get("https://ejemplo.com")

# Encontrar el elemento g con la clase 'mi-clase'
g_element = driver.find_element_by_xpath(
    '/html/body/main/div[1]/div/div[1]/div[4]/div/svg/switch/g[contains(@class, "mi-clase")]'
)

# Realizar alguna acción con el elemento encontrado
g_element.click()

# Cerrar el navegador web
driver.quit()

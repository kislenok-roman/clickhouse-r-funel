# examples

funnel <- list(
  base = CONST_BASE_VORONKI,
  step1 = list(condition = "url like '%wheel\\\\_search\\\\_set\\\\_form%'"),
  step2 = list(condition = c("url like '%wheel\\\\_search\\\\_set\\\\_form%' and url like '%keyName=attributes-based%'",
                             "url like '%wheel\\\\_search\\\\_set\\\\_form%' and url like '%keyName=auto-based%'")),
  step3 = list(condition = c("url like '%/wheel/%' and url not like '%.html' and url not like '%wheelCarModel=%'",
                             "url like '%/wheel/%' and url not like '%.html' and url like '%wheelCarModel=%'")),
  step4 = list(condition = c("url like '%/wheel/%.html' and referrer like '%/wheel/%' and referrer not like '%wheelCarModel=%'",
                             "url like '%/wheel/%.html' and referrer like '%/wheel/%' and referrer like '%wheelCarModel=%'"),
               link = list(list(type = "time less", value = 60))),
  step5 = list(condition = "controller in ('viewContactsController::', 'FarPost/Baza/Offer/PlaceOfferController::', 'FarPost/Baza/Cart/CartController::')",
               #link = list(list(type = "field", fieldThis = "pathFull(referrer)", fieldPrevious = "url")))
               link = list(list(type = "field", fieldThis = "appendTrailingCharIfAbsent(pathFull(referrer), ' ')", fieldPrevious = "appendTrailingCharIfAbsent(url, ' ')")))
  # Замечание: косяк Алексея с пробелами заставляет нас городить хуйню. См. закоменченный пример выше
)


funnel <- list(
  base = CONST_BASE_VORONKI,
  step1 = list(condition = "url like '%viewdir_show_extended_items%' and referrer like '%/wheel/tire/%'"),
  step2 = list(condition = "url like '%viewdir_extended_item_click%' and referrer like '%/wheel/tire/%'"))

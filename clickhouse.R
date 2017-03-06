require(stringr)
require(DBI)
require(clickhouse)

CONST_BASE_VORONKI <- "
   select ring
   from {$localSwitch}.access_log
   where ring not in ('', 'NULL', 'N', 'deleted')
     and length(ring) >= 32
     and date = '{$dt}'
     {$sample}
   group by ring
   having count() between 2 and 10000 and sum(is_bot) = 0
"

funnel <- list(
  base = CONST_BASE_VORONKI,
  step1 = list(condition = "url like '%wheel\\\\_search\\\\_set\\\\_form%'"),
  step2 = list(condition = c("url like '%wheel\\\\_search\\\\_set\\\\_form%' and url like '%keyName=attributes-based%'",
                             "url like '%wheel\\\\_search\\\\_set\\\\_form%' and url like '%keyName=auto-based%'")),
  step3 = list(condition = c("url like '%/wheel/%' and url not like '%.html' and url not like '%wheelCarModel=%'",
                             "url like '%/wheel/%' and url not like '%.html' and url like '%wheelCarModel=%'")),
  step4 = list(condition = c("url like '%/wheel/%.html' and referrer like '%/wheel/%' and referrer not like '%wheelCarModel=%'",
                             "url like '%/wheel/%.html' and referrer like '%/wheel/%' and referrer like '%wheelCarModel=%'"),
               link = list(list(type = "datediff", condition = "< 60"))),
  step5 = list(condition = "controller in ('viewContactsController::', 'FarPost/Baza/Offer/PlaceOfferController::', 'FarPost/Baza/Cart/CartController::')",
               link = list(list(type = "field", field1 = "pathFull(referrer)", field2 = "url")))
)

bindParams <- function(query, ...) {
  params <- list(...)
  names <- names(params)
  if (!is.null(names)) {
    for (name in names) {
      query <- str_replace_all(query, fixed(paste0("{$", name, "}")), params[[name]])
    }
  }
  query
}

createConditions <- function(funel, step, fn = c("uniq", "count", "mindate", "maxdate")) {
  fn <- match.arg(fn)

  if (fn == "uniq") {
    fn <- "uniqIf(ring, "
  } else if (fn == "count") {
    fn <- "countIf("
  } else if (fn == "mindate") {
    fn <- "minIf(datetime, "
  } else if (fn == "maxdate") {
    fn <- "maxIf(datetime, "
  }

  condition <- funel[[step]]$condition

  if (length(condition) == 1) {
    conditions <- paste0(fn, condition, ")")
    names(conditions) <- step
  } else {
    conditions <- c(paste0(fn, condition, ")"),
                    paste0(fn, paste0("(", condition, ")", collapse = " or "), ")"))

    names(conditions) <- c(paste0(step, "_", seq_along(condition)), step)
  }

  conditions
}

printConditions <- function(conditions) {
  paste0(conditions, " as ", names(conditions), collapse = ", ")
}

formatCondition <- function(format, conditions) {
  names <- names(conditions)
  if (is.null(names)) {
    names <- conditions
  }

  conditions <- str_replace(format, fixed("{$condition}"), conditions)
  names(conditions) <- names
  conditions
}

getNextLinks <- function(funnel, i = 1) {
  step <- funnel[[paste0("step", i + 1)]]

  if (!is.null(step) && !is.null(step$link)) {
    # Связь между правилами - со следующим
    links <- c()
    for (link in step$link) {
      if (link$type == "field") {
        # связь по полю
        links <- c(links, link$field1)
      }
    }
    if (length(links) > 0) {
      names(links) <- paste0("link", i, "_", i + 1)
      links
    } else {
      c()
    }
  } else {
    c()
  }
}

getPrevLinks <- function(funnel, i = 2) {
  step <- funnel[[paste0("step", i)]]
  if (!is.null(step) && i > 1 && !is.null(step$link)) {
    # Связь между правилами - с предыдущим
    links <- c()
    for (link in step$link) {
      if (link$type == "field") {
        # связь по полю
        links <- c(links, link$field2)
      }
    }
    if (length(links) > 0) {
      names(links) <- paste0("link", i - 1, "_", i)
      links
    } else {
      c()
    }
  } else {
    c()
  }
}

getInnerQuery <- function(funnel, i = 2) {
  step <- funnel[[paste0("step", i)]]

  prevlinks <- getPrevLinks(funnel, i)
  nextlinks <- getNextLinks(funnel, i)

  if (!is.null(prevlinks)) {
    join <- c("ring", names(prevlinks))
    prevlinks <- paste0(",", printConditions(prevlinks))
  } else {
    join <- c("ring")
    prevlinks <- ""
  }

  if (!is.null(nextlinks)) {
    nextlinks <- paste0(",", printConditions(nextlinks))
  } else {
    nextlinks <- ""
  }

  structure(bindParams("select ring, datetime as datetime{$i} {$links1} {$links2}
from local.access_log
where date = '{$dt}'
  and ({$conditions})
  {$sample}",
                       i = i,
                       links1 = prevlinks,
                       links2 = nextlinks,
                       conditions = paste0("(", step$condition, ")", collapse = " or ")),
            join = join)
}

buildQueryForStep <- function(funnel, i = 1L) {
  steps <- sum(str_detect(names(funnel), regex("step[0-9]+")))

  if (i == 1L) {
    # Особый случай - 1ое правило
    step <- funnel[[paste0("step", i)]]
    links <- getNextLinks(funnel, 1)
    right <- buildQueryForStep(funnel, i + 1)

    if (is.null(links)) {
      links <- ""
    } else {
      links <- paste0(",", printConditions(links))
    }
    otherdates <- paste0("caseWithoutExpr(datetime", (i + 1):steps, " >= datetime", i, ", datetime", (i + 1):steps, ", toDateTime(0)) as datetime", (i + 1):steps, collapse = ",")

    stepsRing <- paste0("uniqIf(ring, ", sapply((i + 1):steps, function(si) {
      paste0("datetime", 1:(si - 1), " <= datetime", 2:(si), collapse = " and ")
    }), ") as step", (i + 1):steps, collapse = ",")

    bindParams("select
  uniq(ring) as step1,
  {$stepsRing}
from (
  select ring, datetime as datetime1, {$otherdates}  {$links}
from bazalogs.access_log
  {$right}
where date = '{$dt}'
  and ({$condition})
  {$sample}
  and {$basecondition}
)",
               stepsRing = stepsRing,
               otherdates = otherdates,
               links = links,
               right = right,
               condition = paste0("(", step$condition, ")", collapse = " or "),
               basecondition = paste0("ring in (", funnel$base, ")"))



  } else if (i == steps) {
    # Особый случай - последнее правило
    query <- getInnerQuery(funnel, i)
    bindParams("all left join (
  {$subquery}
) using {$join}",
               subquery = query,
               join = paste0(attr(query, "join"), collapse = ","))
  } else {
    left <- getInnerQuery(funnel, i)
    right <- buildQueryForStep(funnel, i + 1)
    otherdates <- paste0("caseWithoutExpr(datetime", (i + 1):steps, " >= datetime", i, ", datetime", (i + 1):steps, ", toDateTime(0)) as datetime", (i + 1):steps, collapse = ",")

    bindParams("all left join (
  select distinct ring, datetime{$i}, {$otherdates}
  from (
    {$left}
  )
    {$right}
) using {$join}",
               i = i,
               otherdates = otherdates,
               left = left,
               right = right,
               join = paste0(attr(left, "join"), collapse = ","))
  }
}

getFunnelQuery <- function(funnel, steps = sum(str_detect(names(funnel), regex("step[0-9]+")))) {
  if (steps == 0L) {
    # special case
    structure(bindParams("select count() as base from ({$subquery})",
                         subquery = bindParams(funnel$base, localSwitch = "bazalogs")),
              sample = FALSE)
  } else if (steps == 1L) {
    # special case
    structure(bindParams("select uniq(ring) as base, {$step1}
from bazalogs.access_log
where date = '{$dt}'
  and ring in ({$base})
  {sample}",
                         step1 = printConditions(createConditions(funnel, "step1")),
                         base = funnel$base),
              sample = FALSE)
  } else if (steps == 2L && is.null(funnel[["step2"]]$link)) {
    # special case
    step1 <- createConditions(funnel, "step1", "mindate")
    step2 <- createConditions(funnel, "step2", "maxdate")

    coConoditions <- list()
    for (s1 in names(step1)) {
      for (s2 in names(step2)) {
        coConoditions[[paste0(s1, "__", s2)]] <- paste0(formatCondition("{$condition} <> toDateTime(0)", step1[s1]), " and ", step1[s1], " <= ", step2[s2])
      }
    }
    coConoditions <- unlist(coConoditions)

    structure(bindParams("select count() as base, {step1}, {step2}
from (
  select
    ring, {step1inner}, {step2inner}
  from bazalogs.access_log
  where date = '{$dt}'
    and ring in ({$base})
    {$sample}
  group by ring
)",
                         step1 = printConditions(formatCondition("sum({$condition})", names(step1))),
                         step2 = printConditions(formatCondition("sum({$condition})", names(coConoditions))),
                         step1inner = printConditions(formatCondition("{$condition} <> toDateTime(0)", step1)),
                         step2inner = printConditions(coConoditions),
                         base = funnel$base),
              sample = FALSE)
  } else {
    # common case
    structure(buildQueryForStep(funnel), sample = TRUE)
  }
}

getFunnelDataDay <- function(funnel, dt) {
  if (is.null(funnel$data) || funnel$data[date == dt, sum(sample)] < 1) {
    query <- getFunnelQuery(funnel)

    # param binding
    sample <- attr(query, "sample")
    query <- bindParams(query, dt = dt, localSwitch = "local")

    con <- dbConnect(clickhouse(), host = "clickhouse-n1.dev")
    if (sample) {
      if (is.null(funnel$data)) {
        idx <- 0
      } else {
        idx <- round(funnel$data[date == dt, sample] * 256 + 1)
      }
      sampleCondition <- paste0("and ring like '", format(as.hexmode(idx), 2), "%'")
      data <- dbGetQuery(con,
                         bindParams(query, sample = sampleCondition))
      queryBase <- bindParams(getFunnelQuery(funnel, 0),
                              sample = sampleCondition,
                              dt = dt,
                              localSwitch = "local")
      data <- cbind(data,
                    dbGetQuery(con, queryBase))

      data[, `:=`(date = dt, sample = idx / 256)]
      funnel$data <- rbind(funnel$data,
                           data)[, c(base = sum(base),
                                     sample = max(sample),
                                     lapply(.SD, sum)),
                                 by = date,
                                 .SDcols = grep("step", names(data), value = TRUE)]
    } else {
      data <- dbGetQuery(con, bindParams(query, sample = ""))
      data[, `:=`(date = dt, sample = 1)]
      funnel$data <- rbind(funnel$data, data)
    }
    dbDisconnect(con)
  }

  funnel
}

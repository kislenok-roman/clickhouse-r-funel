require(stringr)
require(DBI)
require(clickhouse)

source("bases.R")

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
        links <- c(links, link$fieldPrevious)
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
        links <- c(links, link$fieldThis)
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

getInnerQuery <- function(funnel, i = 2, allPath = FALSE) {
  if (allPath) {
    stop ("Not implemented")
    # TODO: implement
  }
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

  datetimes <- "datetime as datetime{$i}"
  datetimeNames <- paste0("datetime", i)
  if (length(step$condition) > 1) {
    datetimes <- paste0(datetimes, ",",
                       paste0("caseWithoutExpr(", step$condition, ", datetime, toDateTime(0)) as datetime{$i}_", seq_along(step$condition), collapse = ","))
    datetimeNames <- c(datetimeNames, paste0("datetime", i, "_", seq_along(step$condition)))
  }

  structure(bindParams("select ring, {$datetimes} {$links1} {$links2}
from local.access_log
where date = '{$dt}'
  and ({$conditions})
  {$sample}
  {$split}",
                       datetimes = datetimes,
                       i = i,
                       links1 = prevlinks,
                       links2 = nextlinks,
                       conditions = paste0("(", step$condition, ")", collapse = " or ")),
            join = join,
            datetimes = datetimeNames)
}

buildQueryForStep <- function(funnel, i = 1L, allPath = FALSE) {
  if (allPath) {
    stop("Not implemented")
    # TODO: implement :)
  }

  steps <- sum(str_detect(names(funnel), regex("step[0-9]+")))

  if (i == 1L) {
    # Особый случай - 1ое правило
    step <- funnel[[paste0("step", i)]]
    links <- getNextLinks(funnel, 1)
    right <- buildQueryForStep(funnel, i + 1, allPath)
    if (is.null(links)) {
      links <- ""
    } else {
      links <- paste0(",", printConditions(links))
    }
    datetimes <- "datetime as datetime1"
    datetimeNames <- "datetime1"
    stepsRing <- "uniq(ring) as step1"
    if (length(step$condition) > 1) {
      datetimes <- paste0(datetimes, ",",
                          paste0("caseWithoutExpr(", step$condition, ", datetime, toDateTime(0)) as datetime1_", seq_along(step$condition), collapse = ","))
      datetimeNames <- c(datetimeNames, paste0("datetime1_", seq_along(step$condition)))
      stepsRing <- c(stepsRing,
                     paste0("uniqIf(ring, ", datetimeNames, " <> toDateTime(0)"))
    }

    otherdates <- paste0("caseWithoutExpr(", attr(right, "datetimes"), " >= datetime1, ", attr(right, "datetimes"), ", toDateTime(0)) as ", attr(right, "datetimes"), collapse = ",")

    stepsRing <- c(stepsRing,
                   paste0("uniqIf(ring, ", attr(right, "datetimes"), " <> toDateTime(0)) as ", sub("datetime", "step", attr(right, "datetimes"))))

    bindParams("select
  {$stepsRing}
from (
  select ring, {$datetimes}, {$otherdates}  {$links}
from bazalogs.access_log
  {$right}
where date = '{$dt}'
  and ({$condition})
  {$sample}
  {$split}
  and {$basecondition}
)",
               stepsRing = paste0(stepsRing, collapse = ","),
               datetimes = datetimes,
               otherdates = otherdates,
               links = links,
               right = right,
               condition = paste0("(", step$condition, ")", collapse = " or "),
               basecondition = paste0("ring in (", funnel$base, ")"))



  } else if (i == steps) {
    # Особый случай - последнее правило
    query <- getInnerQuery(funnel, i, allPath)
    structure(bindParams("all left join (
  {$subquery}
) using {$join}",
                         subquery = query,
                         join = paste0(attr(query, "join"), collapse = ",")),
              datetimes = attr(query, "datetimes"))
  } else {
    left <- getInnerQuery(funnel, i, allPath)
    right <- buildQueryForStep(funnel, i + 1, allPath)
    otherdates <- paste0("caseWithoutExpr(", attr(right, "datetimes"), " >= datetime", i, ", ", attr(right, "datetimes"), ", toDateTime(0)) as ", attr(right, "datetimes"), collapse = ",")

    structure(bindParams("all left join (
  select distinct ring, {$datetimes}, {$otherdates}
  from (
    {$left}
  )
    {$right}
) using {$join}",
                         datetimes = paste0(attr(left, "datetimes"), collapse = ","),
                         i = i,
                         otherdates = otherdates,
                         left = left,
                         right = right,
                         join = paste0(attr(left, "join"), collapse = ",")),
              datetimes = c(attr(left, "datetimes"), attr(right, "datetimes")))
  }
}

## TODO: link$type == "time less"
## TODO: link$type == "time more"
getFunnelQuery <- function(funnel,
                           steps = sum(str_detect(names(funnel), regex("step[0-9]+"))),
                           allPath = FALSE) {
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
  {sample}
  {$split}",
                         step1 = printConditions(createConditions(funnel, "step1")),
                         base = funnel$base),
              sample = FALSE)
  } else if (FALSE && steps == 2L && is.null(funnel[["step2"]]$link)) {
    # special case
    step1 <- createConditions(funnel, "step1", "mindate")
    step2 <- createConditions(funnel, "step2", "maxdate")

    coConoditions <- list()
    if (allPath) {
      # INFO: касательно этого места есть мнение, что нам не нужны все переборы - типа напишем если надо
      # Поэтому мы исключаем полный перебор комбинаций и оставляем только прямую связ
      for (s1 in names(step1)) {
        for (s2 in names(step2)) {
          coConoditions[[paste0(s1, "__", s2)]] <- paste0(formatCondition("{$condition} <> toDateTime(0)", step1[s1]), " and ", step1[s1], " <= ", step2[s2])
        }
      }
    } else {
      for (s1 in grep("^step[0-9]+$", names(step1), value = TRUE)) {
        for (s2 in names(step2)) {
          coConoditions[[paste0(s1, "__", s2)]] <- paste0(formatCondition("{$condition} <> toDateTime(0)", step1[s1]), " and ", step1[s1], " <= ", step2[s2])
        }
      }
    }
    coConoditions <- unlist(coConoditions)

    structure(bindParams("select count() as base, {$step1}, {$step2}
from (
  select
    ring, {$step1inner}, {$step2inner}
  from bazalogs.access_log
  where date = '{$dt}'
    and ring in ({$base})
    {$sample}
    {$split}
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
    structure(buildQueryForStep(funnel, 1L, allPath), sample = TRUE)
  }
}

getFunnelDataDay <- function(funnel, dt, split = c("full", "2", "4", "8", "16")) {
  split <- match.arg(split)[1]
  if (split == "full") {
    splitCondition <- ""
  } else {
    # TODO: проблема с пробелами. Привет Алексею
    splitCondition <- paste0("and (", paste0("ring like '%", 16 - 1:(16 / as.integer(split)), " '", collapse = " or "), ")")
  }

  dataname <- paste0("split", split)
  if (is.null(funnel[[dataname]]) || funnel[[dataname]][date == dt, sum(sample)] < 1) {
    query <- getFunnelQuery(funnel)

    # param binding
    sample <- attr(query, "sample")
    query <- bindParams(query, dt = dt, localSwitch = "local")

    con <- dbConnect(clickhouse(), host = "clickhouse-n1.dev")
    if (sample) {
      if (is.null(funnel[[dataname]])) {
        idx <- 0
      } else {
        idx <- funnel[[dataname]][date == dt, sample]
        if (is.null(idx) || length(idx) == 0) {
          idx <- 0
        } else {
          idx <- round(idx * 256)
        }
      }
      sampleCondition <- paste0("and ring like '", format(as.hexmode(idx), 2), "%'")
      query <- bindParams(query, sample = sampleCondition, split = splitCondition)
      queryBase <- bindParams(getFunnelQuery(funnel, 0),
                              sample = sampleCondition,
                              split = splitCondition,
                              dt = dt,
                              localSwitch = "local")
      data <- cbind(dbGetQuery(con, query),
                    dbGetQuery(con, queryBase))

      data[, `:=`(date = dt, sample = (idx + 1) / 256)]
      funnel[[dataname]] <- rbind(funnel[[dataname]],
                                  data)[, c(base = sum(base),
                                            sample = max(sample),
                                            lapply(.SD, sum)),
                                        by = date,
                                        .SDcols = grep("step", names(data), value = TRUE)]
    } else {
      query <- bindParams(query, sample = "", split = splitCondition)
      data <- dbGetQuery(con, query)
      data[, `:=`(date = dt, sample = 1)]
      funnel[[dataname]] <- rbind(funnel[[dataname]] , data)
    }
    dbDisconnect(con)
  }

  funnel
}

getFunnelName <- function(funnel) {
  digest::digest(funnel[grep("base|step", names(funnel), value = TRUE)])
}

saveFunnel <- function(funnel) {
  name <- getFunnelName(funnel)
  saveRDS(funnel, bindParams("funnels/{$name}.RFunnelData", name = name))
  TRUE
}

loadFunnel <- function(name) {
  suppressWarnings(tryCatch(readRDS(bindParams("funnels/{$name}.RFunnelData", name = name)), error = function(e) { NULL }))
}

initFunnel <- function(funnel) {
  name <- getFunnelName(funnel)
  saveFunnel(funnel)
  name
}

putFunnelLock <- function(name) {
  file.create(bindParams("funnels/{$name}.lock", name = name))
}

releaseFunnelLock <- function(name) {
  suppressWarnings(file.remove(bindParams("funnels/{$name}.lock", name = name)))
}

isFunnelLocked <- function(name) {
  file.exists(bindParams("funnels/{$name}.lock", name = name))
}

getFunnelDataPeriod <- function(name, from = Sys.Date() - 1, to = Sys.Date() - 1, split = c("full", "2", "4", "8", "16")) {
  split <- match.arg(split)[1]
  funnel <- loadFunnel(name)
  dataname <- paste0("split", split)

  if (!is.null(funnel)) {
    dts <- as.character(seq(as.Date(from), as.Date(to), "1 day"))
    ord <- c(4, 3, 6, 1, 5, 2, 7) # magic: порядок дней среды, пятницы, понедельники, вс, чт, вт, сб для wday, которая америк. дни возвращает 2 (пн) - 6 (сб), 1 (вс)

    dts <- setorder(data.table(dt = dts, ord = ord[wday(dts)]), ord, -dt)[, dt]

    putFunnelLock(name)
    tryCatch({
      while (length(dts) > 0 && isFunnelLocked(name)) {
        dt <- dts[1]

        funnel <- getFunnelDataDay(funnel, dt)
        if (split != "full") {
          funnel <- getFunnelDataDay(funnel, dt, split)
        }

        if (funnel[[dataname]][date == dt, sample] < 1) {
          # continue
          dts <- c(dts[-1], dts[1])
        } else {
          dts <- dts[-1]
        }

        saveFunnel(funnel)
      }
    }, finally = function() {
      releaseFunnelLock(name)
    })
  } else {
    stop("No funnel found")
  }

  funnel
}

getRingExamples <- function(funnel, dt = Sys.Date() - 1, step = "step1", limit = 10) {

}

getABFunelResult <- function(funnel, from = NULL, to = NULL, lift = 0, step = Inf, split = c("2", "4", "8", "16")) {
  estBetaParams <- function(x) {
    mu <- mean(x)
    var <- 0.005
    alpha <- ((1 - mu) / var - 1 / mu) * mu ^ 2
    beta <- alpha * (1 / mu - 1)
    list(alpha = alpha, beta = beta)
  }

  steps <- sum(str_detect(names(funnel), regex("step[0-9]+")))
  cStep <- paste0("step", pmin(steps, step))
  cPrev <- ifelse(cStep == "step1", "base", paste0("step", pmin(steps, step) - 1))
  split <- match.arg(split)
  splitname <- paste0("split", split)
  if (is.null(from)) {
    from <- funnel$splitfull[, min(date)]
  }
  if (is.null(to)) {
    to <- funnel$splitfull[, max(date)]
  }

  p <- estBetaParams(funnel$splitfull[date >= from & date <= to, get(cStep) / get(cPrev)])
  aConv <- sum(funnel$splitfull[date >= from & date <= to, get(cStep)] - funnel[[splitname]][date >= from & date <= to, get(cStep)])
  aAll <- sum(funnel$splitfull[date >= from & date <= to, get(cPrev)] - funnel[[splitname]][date >= from & date <= to, get(cPrev)])
  bConv <- sum(funnel[[splitname]][date >= from & date <= to, get(cStep)])
  bAll <- sum(funnel[[splitname]][date >= from & date <= to, get(cPrev)])

  a_probs <- rbeta(1e5,
                   aConv + p$alpha,
                   aAll - aConv + p$beta)
  b_probs <- rbeta(1e5,
                   bConv + p$alpha,
                   bAll - bConv + p$beta)

  sampleLift <- (b_probs - a_probs) / a_probs

  list(probBLiftOrMore = mean((100 * sampleLift > lift)),
       liftQuantiles = quantile(sampleLift, c(0.05, 0.5, 0.95)),
       a_info = quantile(a_probs, c(0.025, 0.5, 0.975)),
       b_info = quantile(b_probs, c(0.025, 0.5, 0.975)))
}

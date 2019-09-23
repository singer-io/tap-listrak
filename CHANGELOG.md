# Changelog

## 1.1.0
  * Switches annotated_schema to metadata [#10](https://github.com/singer-io/tap-listrak/pull/10)
  * Adds explicit dependency on `pendulum` version `1.2.0` as a result of #10 [#11](https://github.com/singer-io/tap-listrak/pull/11)

## 1.0.6
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 1.0.5
  * Fixes an error with raising exceptions on a 404 [#3](https://github.com/singer-io/tap-listrak/pull/3)

## 1.0.4
  * Fixes indentation in http request function

## 1.0.3
  * Remove backoff code and skip errors when a 404 comes back [#2](https://github.com/singer-io/tap-listrak/pull/2)

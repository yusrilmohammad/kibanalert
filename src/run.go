package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"kibanalert/alerts"
	"kibanalert/notify"
	"kibanalert/rules"
	"os"
	"strconv"
	"time"
  "net/http"
  "bytes"
  "io/ioutil"
  "log"
)

func main() {
	godotenv.Load()

  client := &http.Client{}
  req, err := http.NewRequest("GET", os.Getenv("ELASTIC_URL") + "/" + os.Getenv("CONNECTOR_INDEX_NAME") + "?pretty", nil)
  req.Header.Set("Authorization", "ApiKey " + os.Getenv("ELASTIC_API_KEY"))
  resp, err := client.Do(req)
  if err != nil {
    log.Fatalln(err)
  }
  //We Read the response body on the line below.
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Fatalln(err)
  }
  //Convert the body to type string
  sb := string(body)
  log.Printf(sb)

  if resp.StatusCode != http.StatusOK {
  
    req, err := http.NewRequest("PUT", os.Getenv("ELASTIC_URL") + "/" + os.Getenv("CONNECTOR_INDEX_NAME") + "?pretty", nil)
    req.Header.Set("Authorization", "ApiKey " + os.Getenv("ELASTIC_API_KEY"))
    resp, err := client.Do(req)
    if err != nil {
      log.Fatalln(err)
    }
    //We Read the response body on the line below.
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
      log.Fatalln(err)
    }
    //Convert the body to type string
    sb := string(body)
    log.Printf(sb)
    if resp.StatusCode == http.StatusOK {
      data := []byte(`{
            "properties": {
              "alert_id": {
                "type": "keyword"
              },
              "date": {
                "type": "date"
              },
              "reason": {
                "type": "text"
              },
              "rule_id": {
                "type": "keyword"
              },
              "service_name": {
                "type": "text"
              }
            }
        }`)
      req, err := http.NewRequest("PUT", os.Getenv("ELASTIC_URL") + "/" + os.Getenv("CONNECTOR_INDEX_NAME") + "/_mapping?pretty", bytes.NewBuffer(data))
      req.Header.Set("Content-Type", "application/json")
      req.Header.Set("Authorization", "ApiKey " + os.Getenv("ELASTIC_API_KEY"))
      resp, err := client.Do(req)
      if err != nil {
          log.Fatalln(err)
      }
    //We Read the response body on the line below.
      body, err := ioutil.ReadAll(resp.Body)
      if err != nil {
          log.Fatalln(err)
      }
    //Convert the body to type string
      sb := string(body)
      log.Printf(sb)

    }
  }

	scanInterval, err := strconv.Atoi(os.Getenv("SCAN_INTERVAL"))
	if err != nil {
		scanInterval = 60
	}

	previousHitId := map[string]string{}
	debug := os.Getenv("DEBUG") == "1"

	for true {
		currentRules := rules.Get(
			os.Getenv("KIBANA_URL"),
			os.Getenv("ELASTIC_API_KEY"),
		)
		for _, rule := range currentRules.Rules {
			if rule.ExecutionStatus.Status == "active" {
				ruleId := rule.RuleId
				if _, ok := previousHitId[ruleId]; !ok {
					previousHitId[ruleId] = ""
				}
				currentAlert := alerts.Get(
					ruleId,
					os.Getenv("CONNECTOR_INDEX_NAME"),
					os.Getenv("ELASTIC_URL"),
					os.Getenv("ELASTIC_API_KEY"),
				)
				if len(currentAlert.Hits.Hits) > 0 {
					hit := currentAlert.Hits.Hits[0]
					if previousHitId[ruleId] != hit.HitId {
						if debug {
							fmt.Println(fmt.Sprintf("Notifying %v hit.HitId: %v", rule.Name, hit.HitId))
						}
						if errs := notify.Notify(hit.Source); errs != nil {
							fmt.Println("Notification Failures: ", errs)
						}
						previousHitId[ruleId] = hit.HitId
					} else {
						if debug {
							fmt.Println(fmt.Sprintf("Skipping %v hit.HitId: %v", rule.Name, hit.HitId))
						}
					}
				}
			}
		}
		time.Sleep(time.Duration(scanInterval) * time.Second)
	}
}

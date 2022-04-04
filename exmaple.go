package awesomeProject

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type hitData struct {
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}

type hit struct {
	Hits []hitData `json:"hits"`
}

type elasticData struct {
	Hits hit `json:"hits"`
}

func stringInSliceS(a string, list []string) bool {
	for _, b := range list {
		if fmt.Sprint(b) == a {
			return true
		}
	}
	return false
}

type multiFieldsParams struct {
	Fields []string `mapstructure:"fields"`
	Value  string   `mapstructure:"value"`
}

//elSearchByAttr
//функция поиска пользователей в elasticSearch
//params для поиска значения в конкретном.
//В multi_fields_params под ключом fields отправлется массив полей и под ключом value значение, которое надо найти среди этих полей. Это нужно, когда из одного поля на фронте нужно искать сразу по нескольким полям, как в случае поиска по ФИО и организации из одного поля.
func elSearchByAttr(params map[string]interface{}, mfParams []multiFieldsParams) ([]map[string]interface{}, error) {

	//Если пустые параметры, возвращаемся
	if params == nil && len(mfParams) == 0 {
		return nil, nil
	}

	//поля по которым нужно использовать неточное совпадение
	var fuzzyFields = []string{"city", "first_name", "last_name", "middle_name", "organisation"}

	request := `{ "query": { "bool": {"must": [ `
	//кладем параметры для поиска "поле":"значение"
	for key, val := range params {
		if stringInSliceS(key, fuzzyFields) {
			request += fmt.Sprintf(
				` { "match": { "%s": { "query": "%v","fuzziness": 2 }}},`, key, val)
		} else {
			request += fmt.Sprintf(
				` {"match_phrase": {"%s": "%v"}},`, key, val)
		}
	}

	//кладем параметры для поиска по нескольким полям
	for _, multiFields := range mfParams {
		var fields string
		var fuzzy = true
		for _, field := range multiFields.Fields {
			if !stringInSliceS(field, fuzzyFields) {
				fuzzy = false
			}
			fields += fmt.Sprintf(`"%s",`, field)
		}
		var multiMatchQuery string
		if fuzzy {
			multiMatchQuery = fmt.Sprintf(`
			{"multi_match": {
				"query": "%s",
				"type": "best_fields", "fuzziness": 2, "fields": [%s`, multiFields.Value, fields)
		} else {
			multiMatchQuery = fmt.Sprintf(`
			{"multi_match": {
				"query": "%v",
				"type": "best_fields", "fields": [%s`, multiFields.Value, fields)
		}
		multiMatchQuery = multiMatchQuery[:len(multiMatchQuery)-1]
		multiMatchQuery += "]}},"
		request += multiMatchQuery
	}
	request = request[:len(request)-1]
	request += "]}}}"

	client := http.Client{}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:%s/rk5_users/_search/?size=10000", dbl.ElConfig.Ip, dbl.ElConfig.Port), bytes.NewBuffer([]byte(request)))
	if err != nil {
		return nil, fmt.Errorf("http.newRequest err: %s", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", dbl.ElasticPass))
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("sending request to elastic err: %s", err)
	}
	if resp.Status != "200 OK" {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("elastic response status is not 200: %s", string(bodyBytes))
	}
	var elasticResponse = new(elasticData)
	err = json.NewDecoder(resp.Body).Decode(elasticResponse)
	if err != nil {
		return nil, fmt.Errorf("decoding elastic resp err, %s", err)
	}
	result := make([]map[string]interface{}, 0)
	for _, v := range elasticResponse.Hits.Hits {
		result = append(result, v.Source)
	}

	return result, nil
}

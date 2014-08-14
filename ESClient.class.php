<?php
/**
 * Yaec: Yet Another ElasticSearch Client (v0.1)  -  27/March/2014
 * 
 * Copyright (C) 2014  Ernesto Giralt (egiralt@gmail.com) 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
namespace Yaec;

DEFINE ('ERROR_EMPTY_RESPONSE', 'Valores no encontrados');
DEFINE ('ERROR_INVALID_SEARCH', 'Búsqueda no válida');

use \Yaec\Exceptions as Exception;
/**
 * Clase principal 
 */
 class Yaec_ESClient
 {
 	const ELASTICSEARCH_DEFAULT_PORT = 9200;
	const ELASTICSEARCH_DEFAULT_SERVER = 'localhost';
	
	private $Index;
	private $Server				= 'localhost';
	private $Port				= 9200;
	private $ScriptsDirectory 	= 'scripts';
	
	public function __construct ($indexName, $serverAddress = null, $port = null)
	{
		if (!isset($indexName))
			throw new Exception\Yaec_NoDefaultIndexException ();
			
		$this->SetServer ( isset($serverAddress) ? $serverAddress : Yaec_ESClient::ELASTICSEARCH_DEFAULT_SERVER);
		$this->SetPort( isset($port) ? $port : Yaec_ESClient::ELASTICSEARCH_DEFAULT_PORT);
		$this->SetDefaultIndex($indexName);
	}

	public function SetPort ($port)
	{
		$this->Port = $port;	
	}

	public function GetPort()
	{
		return $this->Port;	
	}
	
	public function SetServer ($serverAddr)
	{
		$this->Server = $serverAddr;
	}
	
	public function GetServer ()
	{
		return $this->Server;
	}
	
	public function SetDefaultIndex ($indexName)
	{
		$this->Index = $indexName;	
	}
		
	public function GetDefaultIndex()
	{
		return $this->Index;
	}
	
	/**
	 * 
	 */
	public function DoCompletionSuggest ($search, $field, $routing = null)
	{
		$result = array();
		
		$query->suggest_query = new \stdClass();
		$query->suggest_query->text = $search;
		$query->suggest_query->completion->field = $field;
		$url = $this->BuildSuggestURL($routing);
		
		$queryResults = $this->DoQuery($query, null, $url,$routing, "POST");
		$optionsList = $queryResults->suggest_query[0]->options;
		
		return $optionsList;
	}
	
	public function GetMapping ($type)
	{
		$result = null;
		$url = sprintf('%s/_mapping/%s',$this->GetBaseUrl (), $type);
		
		$mapping = $this->DoQuery (null,null, $url);
		if (isset($mapping->{$this->GetIndexName()}))
			$result = $mapping->{$this->GetIndexName()}->mappings->$type;
		 
		return $result;
	}

	/**
	 * 
	 */
	public function DoUpdate ($document, $key, $type, $routing = null)
	{
		$docToUpdate = new \stdClass();
		$docToUpdate->doc = $document;
		
		$url = $this->BuildUpdateURL ($key, $type, $routing);
		
		$response = $this->DoQuery ($docToUpdate, null, $url, $routing);
		
		return $result;			
	}
	
	
	public function GetItem ($type, $id, $routing = null)
	{
		$result = null;
		
		$url = sprintf ('%s/%s/%s', $this->GetBaseUrl(), $type, $id);
		
		if ($routing != null)
			$url .= '?routing='.$routing;
		
		$data = $this->DoQuery (null, null, $url);
		
		if ($data->found)
			$result = $data->_source;
		
		return $result;
	}
	
	/**
	 * Usado para lanzar las consultas
	 * 
	 */
	public function DoQuery($query, $type = null, $url = null, $routing = null, $method = "GET")
	{
		if ($type === null && $url === null && $this->_url === null)
			throw new Exception("Se requiere indicar un tipo del repositorio. Use el constructor de la clase o índiquelo en el momento de la búsqueda", 1);
		elseif ($type != null && $url == null)
			$url = $this->BuildSearchQueryURL($type, false, $routing);
			
		$result = null;
		if (!empty($query))
			$data = json_encode($query,JSON_UNESCAPED_SLASHES);
		else 
			$data = '';
		
		$ch = curl_init();
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_PORT, $this->GetPort());	
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
		curl_setopt($ch, CURLOPT_URL, $url);
		curl_setopt($ch, CURLOPT_HTTPHEADER, 
			array(
			'Content-Type: application/json',
			'Accept: application/json'
			));
		if (!empty($data))
			curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
		
		//make the request
		$response = curl_exec($ch);
		if (empty($response))
		{
			$errobj = new \stdClass();
			$errobj->error_number =curl_errno($ch);
			$errobj->error =curl_error($ch);
			$response = json_encode($errobj); 
		};
		
		curl_close($ch); // Se cierra la consulta
		
		$result = json_decode($response);
		
		return $result;
	}


	 /**
	  * Retorna una lista de elementos elemento
	  */
	 public function MatchMany ($type, $searchValue, $routing = null, $count = null)
	 {
	 	$customURL = $this->BuildSearchQueryURL($type, false, $routing);
	 	
	 	if (is_array($searchValue) && count($searchValue) > 1)
		{// Si es una búsqueda de varios campos, se usa una búsqueda múltiple
		 	$query->query->bool->must = array();
		 	foreach ($searchValue as $key => $value )
			{
				$match = new \stdClass();
		 		$match->match->$key = ($value === null ? '' : $value);// Hay que garantizar que no de error por un null mal puesto
		 		$query->query->bool->must[] = $match;
			}
		}
		else 
		{
			// Se usa el operador match simple!
			$field = is_array($searchValue) ? array_keys($searchValue)[0] : '_id';
			$value = is_array($searchValue) ? array_values($searchValue)[0] : $searchValue;
			
		 	$query->query->match->$field = ($value !== null ? $value : '');// Hay que garantizar que no de error por un null mal puesto
		}
		
		if ($count !==  null && is_numeric($count))
			$query->size = $count;

		print_r ($query);						
		$resultSet = $this->DoQuery($query, $type, $customURL);
		
		$result = array();
		if ($resultSet && $resultSet->hits->total > 0)
		{
			foreach ($resultSet->hits->hits as $item)			
				$result[] = $item->_source;
		}
		
		//echo "<pre>"; print_r ($result); die();
		return $result;
	 }

	 /**
	  * Retorna el primer elemento que cumpla con el valor en el campo específicado
	  */
	 public function MatchOne ($type, $searchValue, $routing = null)
	 {
	 	$result = null;
	 	$resultSet = $this->MatchMany($type, $searchValue, $routing, 1);
		
		if ($resultSet && count($resultSet) > 0)
			$result = $resultSet[0];			
		 
		return $result;
	 }
	
	/**
	 * Ejecuta un script almacenado en /include/ElasticSearch/scripts y devuelve el resultado.
	 */
	function ExecScript ($scriptFileName, $paramsArray = array(), $type = null, $url = null, $routing = null)
	{
		if (!file_exists($scriptFileName))
			throw new Exception\Yaec_ScriptNotFoundException(); 
		
		// Leer y sustituir todos los parámetros que se encuentran allí
		$jsonSource = file_get_contents($scriptFileName);
		$eResult = array();
		$today = new DateTime();
		if (preg_match_all("/\{\{([^\}]*)\}\}/", $jsonSource, $eResult ))
		{
			foreach ($eResult[1] as $foundParam) // Se toman todos los patrones encontrados 
			{
				$value = null;
				$foundParam = trim($foundParam); 
				if (substr($foundParam, 0, 1) == '#') // Es una metavariable?
				{
					$metas = explode(',', $foundParam);
					switch (trim($metas[0]))
					{
						case '#today' :
						case '#current_date' :
							if (count($metas) > 1)
								$value = $today->format(trim($metas[1]));
							else 
								throw new Exception("Se requiere el parámetro <format> para #current_date", 1000);
							break;
						case '#yesterday' :
							if (count($metas) > 1)
							{
								$today->sub (new DateInterval ('P1D'));
								$value = $today->format(trim($metas[1]));
							}
							else 
								throw new Exception("Se requiere el parámetro <format> para #current_date", 1000);
							break;
					}
				}
				else
					if (!empty($paramsArray)) 
						$value = $paramsArray [trim($foundParam)];

				if ($value !== null)
					$replaceString = '{{'.$foundParam.'}}';
				
				$jsonSource = str_replace($replaceString, $value, $jsonSource);
			} // foreach
		} // if

		$jsonObj = json_decode($jsonSource);		
		if ($jsonObj == null)
			throw new Exception("Script inválido", 1);
	
		$result = $this->DoQuery($jsonObj, $type, $url, $routing);
		
		return $result;				
	}
	
	 public function GetTypeCount ($type, $keyfield, $routing)
	 {
	 	$customURL = $this->BuildSearchQueryURL($type, false, $routing);
		
 		$query = new \stdClass();
		$query->aggs->type_count->cardinality->field = $keyfield;
		$query->size = 0;
		
		$resultSet = $this->DoQuery($query, $type, $customURL);
		$result = null;
		if ($resultSet && !$resultSet->error)
		{
			$result = $resultSet->aggregations->type_count->value;
		}
		
		return $result;
	 }

	/**
	 * Retorna una lista de variable
	 */	 
	 public function GetValueList ($variable, $partialTerm = null, $includeCount = true,  $limit = 10, $routing = null)
	 {
	 	$dotPos = strpos($variable, ".");
		$rawVariable = $dotPos > 0 ? substr($variable, $dotPos + 1 ) : $variable;
		$type = substr($variable, 0, strpos($variable, ".") );

	 	$query = new \stdClass();
		$query->size = 0; // No se desean resultados de búsquedas
		$query->query->match_all = new \stdClass(); // se buscan todos los documentos
		$query->aggs->variables->terms->script = "_source['$rawVariable']";
		$query->aggs->variables->terms->order->_term = "asc";
		if ($partialTerm != null)
		{
			$partialTerm = str_replace(array('.', '*'), array('\.', '\*'), $partialTerm);
			$query->aggs->variables->terms->include->pattern = ".*$partialTerm.*";
			$query->aggs->variables->terms->include->flags = "CANON_EQ|CASE_INSENSITIVE";
			$query->aggs->variables->terms->size = $limit;
			$query->aggs->variables->terms->order->_term = 'asc'; // Ordenar por el valor del campo, de A a Z
		}

		$listResult = $this->DoQuery($query, $type, null, $routing);
		$result = array();
		if (!empty($listResult) && !isset($listResult->error) && count($listResult->aggregations->variables->buckets) > 0)
			foreach ($listResult->aggregations->variables->buckets as $bucket)
			{
				$item = null;
				if ($includeCount)
				{
					$item = new \stdClass();
					$item->value = $bucket->key;
					$item->text = $bucket->key;
					$item->count = $bucket->doc_count;
				}
				else 
					$item = $bucket->key;
				
				$result[] = $item;		
			}
			
		// Se ha encontrado algo?
		if (empty($result))
		{
		 	$result = new \stdClass();
			$result->empty = true;
		}
		
		return $result;			
	 }
	
//****************************************  Funciones para almacenar documentos ******************************/

	public function SaveItem ($type, $item, $id,$routing = null)
	{
		$url = $this->BuildSaveQuery($type, $id, $routing);
		return $this->DoQuery($item, null, $url, $routing, 'PUT');
	} 
	
//************************************** funciones para borrar documentos ************************************/


	public function DeleteItem ($type, $item, $id, $routing = null)
	{
		$url = $this->BuildDeleteQuery($type, $id, $routing);
		return $this->DoQuery(null, null, $url, $routing, 'DELETE');
	} 
	
/***************************** Construir las URLs según el tipo de operacion *******************************************/

	/**
	 * Retorna la base del URL, incluyendo el servidor y el puerto.
	 */
	protected function GetBaseUrl ()
	{
		if ($this->GetServer() == null || $this->GetPort() == null)
			throw new Exception\Yaec_BadConnectionParametersException();
		
		return sprintf ('%s:%s/%s', $this->GetServer(), $this->GetPort(), $this->GetDefaultIndex());		
	}	 	
	
	/**
	 * 
	 */
	 protected function BuildSearchQueryURL ($type = null, $isCount = false, $routing = null) 
	 {
	 	$base_url = $this->GetBaseUrl ();
		
		if (!empty($type)) // Si se indica un type, se modifica el URL
			$result = sprintf ('%s/%s/_search', $base_url, $type);
		else
			$result = $base_url.'/_search';
		
		$part = array();
		if ($isCount === true)
			$part[] = "search_type=count";
		if (!is_null($routing))
			$part[] = "routing=".$routing;
		
		if (count($part) > 0)
			$result .= '?'. join ("&", $part);
		
		return $result;
	 }

	/**
	 * 
	 */
	 protected function BuildSuggestURL ($routing = null)
	 {
	 	$result = sprintf('%s/_suggest', $this->GetBaseUrl());
		
		if ($routing != null)
			$result .= "?routing=$routing";
		
		return $result;	
	 } 
	
	protected function BuildDeleteQuery ($type, $id, $routing = null)
	{
		$result = $this->GetBaseUrl ().sprintf('/%s/%s', $type, $id);
		if ($routing !== null)
		{
			$result .= '?routing='.$routing;
		}	
		
		return $result;
	}	

	protected function BuildSaveQuery ($type, $id, $routing = null)
	{
		$result = sprintf('%s/%s/%s',$this->GetBaseUrl (), $type, $id);
		if ($routing !== null)
		{
			$result .= '?routing='.$routing;
		}	
		
		return $result;
	}


	/**
	 * 
	 */
	 protected function BuildUpdateURL ($key, $type, $routing = null)
	 {
	 	$result = sprintf('%s/%s/%s/_update', $this->GetBaseUrl(), $key);
		
		if ($routing != null)
			$result .= "?routing=$routing";
		
		return $result;	
	 } 
		
 
} // class

	
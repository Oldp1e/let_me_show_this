<?php
$v_data = $p_data;
$v_lookup_source = lc_array_change_case($p_lookup_source,'UPPER');
$v_return_data = [];

$external_lookups = [];
$lookups_query_source = [];
$needed_aux_columns = [];
$aux_columns_query_base = [];
foreach($v_data as $index => $row){
	foreach(array_keys($row) as $column){
		$value = $v_data[$index][$column];
		$lookup = isset($v_lookup_source[$column]['OPTIONS'][strtoupper($value)]) ? $v_lookup_source[$column]['OPTIONS'][strtoupper($value)] : '';
		$delimiter = isset($v_lookup_source[$column]['DELIMITER']) ? $v_lookup_source[$column]['DELIMITER'] : '';		
		$action = isset($v_lookup_source[$column]['ACTION']) ? $v_lookup_source[$column]['ACTION'] : '';
		if(empty($action)){			
			// Caso não haja uma ação de banco para esse valor ele é inserido olhando na Lookup Source
			if(!empty($delimiter)){
				$value_temp = explode($delimiter, $value);
				$lookup = [];
				foreach($value_temp as $val_key_lookup){
					$lookup_temp = isset($v_lookup_source[$column]['OPTIONS'][strtoupper($val_key_lookup)]) ? $v_lookup_source[$column]['OPTIONS'][strtoupper($val_key_lookup)] : '';
					$lookup[] = $lookup_temp;								
				}
				$lookup = implode($delimiter.' ',$lookup);
			}
			$v_data[$index][$column] = [];
			$v_data[$index][$column]['value'] = $value;
			$v_data[$index][$column]['lookup'] = $lookup;
		}else{
			$action_name = 'm_'.$action;
			$v_query = $this->$action_name(true);
			$lookup_required_info = [];
			$lookup_required_info['QUERY'] = $v_query;
			$lookup_required_info['VALUE'] = $value;
			$lookup_required_info['COLUMN'] = $column;
			$lookup_required_info['AUX_COLUMNS'] = [];				
			if(!empty($v_lookup_source[$column]['AUXCOLUMNS'])){			
				$needed_aux_columns[$index][$column] = $value;
				if(!isset($aux_columns_query_base[$column])){
					$aux_columns_query_base[$column] = $v_query;
				}				
				foreach($v_lookup_source[$column]['AUXCOLUMNS'] as $aux_column){
					$needed_aux_columns[$index][strtoupper($aux_column)] = $row[strtoupper($aux_column)];
				}
			}
			if(!in_array($lookup_required_info, $lookups_query_source)){
				$lookups_query_source[] = $lookup_required_info;
			}
		}
	}	 
}
//-----------------------------------------------------------------------------------------Lookups sem Auxcolumns------------------------------------------
$agrouped_columns_no_aux = [];
$agrouped_columns_with_aux = [];
foreach($lookups_query_source as ['COLUMN' => $column, 'VALUE' => $value, 'AUX_COLUMNS' => $aux_columns, 'QUERY' => $query]){
	//$index = $source['INDEX'];
	// Agrupando valores da source que não tem aux_columns
	if(count($aux_columns) <= 0){		
		if(isset($agrouped_columns_no_aux[$column]['LOOKUP_IDS']) && !in_array("'".$value."'",$agrouped_columns_no_aux[$column]['LOOKUP_IDS'])){
			$agrouped_columns_no_aux[$column]['LOOKUP_IDS'][] = "'".$value."'";
		}		
		$agrouped_columns_no_aux[$column]['QUERY'] = $query;		
	}
}

$v_query = '';
$v_lookup_queries = [];
foreach(array_keys($agrouped_columns_no_aux) as $column_to_build_query){
	if(isset($agrouped_columns_no_aux[$column_to_build_query]['LOOKUP_IDS'])){
		$v_query = "
			SELECT * FROM ( ".$agrouped_columns_no_aux[$column_to_build_query]['QUERY']." ) 
			WHERE \"id\" IN (".join(',',$agrouped_columns_no_aux[$column_to_build_query]['LOOKUP_IDS']).")
		";
		$v_lookup_queries[$column_to_build_query]['QUERY'] = $v_query;
	}
}

$v_lookup_source_external = [];
foreach(array_keys($v_lookup_queries) as $column_query_info){
	$query_results = gt_select($v_lookup_queries[$column_query_info]['QUERY']);
	foreach($query_results as $result){
		$v_lookup_source_external[$column_query_info]['OPTIONS'][$result['id']] = $result['text'];  
	}
}
//-----------------------------------------------------------------------------------------Fim Lookups sem Auxcolumns------------------------------------------
// Gerenciamento dos lookups com aux columns
// $needed_aux_columns exemplo =>[ 
// 	 [0] = [COD_ITEM => ITEM001, ESTAB=>'BELA_VISTA', 'COD_LOCAL'=>'ARMAZEM01']
// 	 [1] = [COD_ITEM => ITEM002, ESTAB=>'BELA_VISTA', 'COD_LOCAL'=>'ARMAZEM01']
//   [2] = [COD_ITEM => ITEM001, ESTAB=>'VISLUMBRA', 'COD_LOCAL'=>'ARMAZEM_01']
// 	 [3] = [COD_ESTAB => ITEM001, COMPANY=>'2']
// ] 
$aux_columns_info_for_query_building = [];
$aux_columns_info_distinct = [];
// Loop atráves do objeto montado para verificar todos os registros que tem aux_column e irá agrupa-los em um objeto chamado $aux_columns_info_for_query_building
foreach($needed_aux_columns as $index=>$aux_info){
	$aux_info_removed_key = array_merge(array(), $aux_info);
	$first_key = array_keys($aux_info_removed_key)[0]; // Nome da coluna que está sendo trabalhado o lookup
	unset($aux_info_removed_key[$first_key]);
	
	if(!isset($aux_columns_info_distinct[$first_key])){
		$aux_columns_info_distinct[$first_key] = [];
	}
	
	if(!in_array($aux_info_removed_key, $aux_columns_info_distinct[$first_key])){
		$aux_columns_info_distinct[$first_key][] = $aux_info_removed_key;
		$aux_columns_info_for_query_building[$first_key][] = $aux_info_removed_key; // Agrupando colunas auxiliares distintas
		$index_for_query_build = count($aux_columns_info_for_query_building[$first_key]) - 1;
		
		$aux_columns_info_for_query_building[$first_key][$index_for_query_build]['IN'] = [];
		
		if(!in_array($aux_info[$first_key], $aux_columns_info_for_query_building[$first_key][$index_for_query_build]['IN'])){
			$aux_columns_info_for_query_building[$first_key][$index_for_query_build]['IN'][] = "'".$aux_info[$first_key]."'";// Agrupando itens
		}
		
	}else{
		$position_to_insert = array_search($aux_info_removed_key, $aux_columns_info_distinct[$first_key]);
		
		if(!in_array($aux_info[$first_key], $aux_columns_info_for_query_building[$first_key][$position_to_insert]['IN'])){
			$aux_columns_info_for_query_building[$first_key][$position_to_insert]['IN'][] = "'".$aux_info[$first_key]."'";// Agrupando itens
		}
		
	}
}
//  $aux_columns_info_for_query_building => 
// [0] => [[COD_ITEM_PK] => [ESTAB => 'BELA_VISTA', 'COD_LOCAL'=>'ARMAZEM01',  'IN' => ITEM001, ITEM002]]
// [1] => [ESTAB =>'VISLUMBRA', 'COD_LOCAL'=>'ARMAZEM_01'] 

// Para cada agrupamento gerado irá montar uma query para aquela coluna em especifico
$v_queries_to_run = [];
foreach($aux_columns_info_for_query_building as $column_name=>$query_info){
	foreach($query_info as $query_base){
		$v_replaced_query = $aux_columns_query_base[$column_name];
		$v_replaced = $v_replaced_query;
		foreach($query_base as $column_aux_name=>$aux_value){
			if(strcmp($column_aux_name, 'IN') !== 0){
				// Substitui o valor que esta na coluna auxiliar na clausula where da query de select que veio da base para essa coluna no lookup
				$v_replaced = str_replace(strtolower($column_aux_name)." = ''",strtolower($column_aux_name)." = '".$aux_value."'",$v_replaced);				
			}
		}		
		if(!isset($v_queries_to_run[$column_name])){
			$v_queries_to_run[$column_name] = [];
		}
		// Monta as queries que serão executadas para cada colunas especifica da tabela
		$v_queries_to_run[$column_name][]['QUERY'] = "SELECT * FROM (".$v_replaced.") WHERE \"id\" IN (".join(',',$query_base['IN']).")";		
		$v_queries_to_run[$column_name][count($v_queries_to_run[$column_name]) - 1]['DEPARA'] = $query_base;
	}	
}



$results = [];
foreach($v_queries_to_run as $column_name=>$select_info){
	foreach($select_info as $select_query){
		
		if(!isset($results[$column_name])){
			$results[$column_name] = []; 
		}
		$results[$column_name][]['VALUES'] = gt_select($select_query['QUERY']);
		unset($select_query['DEPARA']['IN']);
		$results[$column_name][count($results[$column_name]) - 1]['DEPARA'] = $select_query['DEPARA'];
	}		
}


foreach($v_data as $index => $row){
	foreach(array_keys($row) as $column){
		foreach($results as $column_lookup=>$lookup_info){
			if($column == $column_lookup){											
				foreach($lookup_info as $info_for_comparison){
					$comparison_result = true;
					foreach($info_for_comparison['DEPARA'] as $column_for_comparison=>$column_for_comparison_value){						
						if($column_for_comparison_value != $row[$column_for_comparison]['value']){							
							$comparison_result = false;
						}
					}
					if($comparison_result == true){
						foreach($info_for_comparison['VALUES'] as $value_to_insert_lookup){
							$result_compare_table = $row[$column];
							if(!isset($v_data[$index][$column]['lookup'])){
								$v_data[$index][$column] = [];
							}
							$v_data[$index][$column]['value'] = $result_compare_table;
							
							if(strcmp($result_compare_table, $value_to_insert_lookup['id']) == 0){
								$v_data[$index][$column]['lookup'] = $value_to_insert_lookup['text'];
							}							
						}
					}
				}
			}			
		}
	}
}


return $v_data;
?>

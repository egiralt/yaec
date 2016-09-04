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
namespace Yaec\YaecBundle\Exceptions;

class QueryErrorException extends BaseException
{
    // Redefine the exception so message isn't optional
    public function __construct($message = null, Exception $previous = null) 
    {
		//TODO:Habrá alguna biblioteca que usar para las traducciones?
		if (!$message)        
			$message = 'Query error not specified';
			
        parent::__construct($message, -1, $previous);
    }

}

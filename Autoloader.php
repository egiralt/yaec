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

class Yaec_Loader
{
    /**
     *
     */
    public static function register()
    {
    	
        if (version_compare(phpversion(), '5.3.0', '>=')) {
            spl_autoload_register(array(__CLASS__, '__autoload'), true, true);
        } else {
            spl_autoload_register(array(__CLASS__, '__autoload'));
        }
    }

    public static function __autoload($class)
    {
        if (0 !== strpos($class, 'Yaec\\')) {
            return;
        }
        
        $result = preg_match ('/(^.*)(Yaec_\w+)$/', $class, $matches);
		$file = dirname(__FILE__)
			.preg_replace ('@\\@','', $matches[1])
			.'/'.preg_replace ('/Yaec_/', '', $matches[2])
			.'.class.php';
		
        if (is_file($file))
		{
            require_once $file;
        }
    }
}

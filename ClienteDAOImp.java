/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.ClienteDAO;
import sys.model.AgmaePersona;

import sys.util.Service;

public class ClienteDAOImp implements ClienteDAO {

    @Override
    public List<AgmaePersona> listarClientes() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaePersona cliente = null;
        List<AgmaePersona> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT cod_persona, cod_anexo, Nom_Razon_Social, "
                    + "cod_identificacion, num_identificacion, "
                    + "flg_indicador_domiciliado, cod_departamento, cod_provincia, "
                    + "cod_distrito, cod_zona, cod_via, des_apellido_paterno, "
                    + "des_apellido_materno, des_nombre, tip_sexo, fec_nacimiento, "
                    + "num_edad, cod_pais, cod_departamento_nac, cod_provincia_nac, "
                    + "num_telefono, num_fax, des_email, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion FROM agmae_persona "
                    + "WHERE cod_persona IN (SELECT agmae_persona.cod_persona "
                    + "FROM Agmae_Tipos_Anexos INNER JOIN ((agmae_persona INNER "
                    + "JOIN Agmae_Tipos_Persona ON agmae_persona.tip_persona = "
                    + "Agmae_Tipos_Persona.tip_persona) INNER JOIN agmae_anexos "
                    + "ON agmae_persona.cod_persona = agmae_anexos.cod_persona) "
                    + "ON Agmae_Tipos_Anexos.Tip_anexo = agmae_anexos.Tip_anexo "
                    + "WHERE (((Agmae_Tipos_Anexos.des_tipo_anexo)='CLIENTES')));";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                cliente = new AgmaePersona();
                cliente.setCodPersona(rs.getInt("cod_persona"));
                cliente.setCodAnexo(rs.getString("cod_anexo"));
                cliente.setNomRazonSocial(rs.getString("Nom_Razon_Social"));
                cliente.setCodIdentificacion(rs.getInt("cod_identificacion"));
                cliente.setNumIdentificacion(rs.getString("num_identificacion"));
                cliente.setFlgIndicadorDomiciliado(rs.getString("flg_indicador_domiciliado"));
                cliente.setCodDepartamento(rs.getInt("cod_departamento"));
                cliente.setCodProvincia(rs.getInt("cod_provincia"));
                cliente.setCodDistrito(rs.getInt("cod_distrito"));
                cliente.setCodZona(rs.getInt("cod_zona"));
                cliente.setCodVia(rs.getInt("cod_via"));
                cliente.setDesApellidoPaterno(rs.getString("des_apellido_paterno"));
                cliente.setDesApellidoMaterno(rs.getString("des_apellido_materno"));
                cliente.setDesNombre(rs.getString("des_nombre"));
                cliente.setTipSexo(rs.getString("tip_sexo"));
                cliente.setFecNacimiento(rs.getString("fec_nacimiento"));
                cliente.setNumEdad(rs.getInt("num_edad"));
                cliente.setCodPais(rs.getString("cod_pais"));
                cliente.setCodDepartamentoNac(rs.getInt("cod_departamento_nac"));
                cliente.setCodProvincia(rs.getInt("cod_provincia_nac"));
                cliente.setNumTelefono(rs.getString("num_telefono"));
                cliente.setNumFax(rs.getString("num_fax"));
                cliente.setDesEmail(rs.getString("des_email"));
                cliente.setFecCreacion(rs.getString("fec_creacion"));
                cliente.setHorCreacion(rs.getString("hor_creacion"));
                cliente.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                cliente.setFecActualizacion(rs.getString("fec_actualizacion"));
                cliente.setHorActualizacion(rs.getString("hor_actualizacion"));
                cliente.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

                lista.add(cliente);
            }
            if (!existe) {
                cliente = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    @Override
    public AgmaePersona consultarObjCliente(int codCliente) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaePersona cliente = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT cod_persona, cod_anexo, Nom_Razon_Social, "
                    + "cod_identificacion, num_identificacion, "
                    + "flg_indicador_domiciliado, cod_departamento, cod_provincia, "
                    + "cod_distrito, cod_zona, cod_via, des_apellido_paterno, "
                    + "des_apellido_materno, des_nombre, tip_sexo, fec_nacimiento, "
                    + "num_edad, cod_pais, cod_departamento_nac, cod_provincia_nac, "
                    + "num_telefono, num_fax, des_email, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion FROM agmae_persona "
                    + "WHERE cod_persona IN (SELECT agmae_persona.cod_persona "
                    + "FROM Agmae_Tipos_Anexos INNER JOIN ((agmae_persona INNER "
                    + "JOIN Agmae_Tipos_Persona ON agmae_persona.tip_persona = "
                    + "Agmae_Tipos_Persona.tip_persona) INNER JOIN agmae_anexos "
                    + "ON agmae_persona.cod_persona = agmae_anexos.cod_persona) "
                    + "ON Agmae_Tipos_Anexos.Tip_anexo = agmae_anexos.Tip_anexo "
                    + "WHERE (((Agmae_Tipos_Anexos.des_tipo_anexo)='CLIENTES'))) AND cod_persona=?;";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, codCliente);
            rs = ps.executeQuery();

            cliente = new AgmaePersona();
            if (rs.next()) {
                cliente.setCodPersona(rs.getInt("cod_persona"));
                cliente.setCodAnexo(rs.getString("cod_anexo"));
                cliente.setNomRazonSocial(rs.getString("Nom_Razon_Social"));
                cliente.setCodIdentificacion(rs.getInt("cod_identificacion"));
                cliente.setNumIdentificacion(rs.getString("num_identificacion"));
                cliente.setFlgIndicadorDomiciliado(rs.getString("flg_indicador_domiciliado"));
                cliente.setCodDepartamento(rs.getInt("cod_departamento"));
                cliente.setCodProvincia(rs.getInt("cod_provincia"));
                cliente.setCodDistrito(rs.getInt("cod_distrito"));
                cliente.setCodZona(rs.getInt("cod_zona"));
                cliente.setCodVia(rs.getInt("cod_via"));
                cliente.setDesApellidoPaterno(rs.getString("des_apellido_paterno"));
                cliente.setDesApellidoMaterno(rs.getString("des_apellido_materno"));
                cliente.setDesNombre(rs.getString("des_nombre"));
                cliente.setTipSexo(rs.getString("tip_sexo"));
                cliente.setFecNacimiento(rs.getString("fec_nacimiento"));
                cliente.setNumEdad(rs.getInt("num_edad"));
                cliente.setCodPais(rs.getString("cod_pais"));
                cliente.setCodDepartamentoNac(rs.getInt("cod_departamento_nac"));
                cliente.setCodProvincia(rs.getInt("cod_provincia_nac"));
                cliente.setNumTelefono(rs.getString("num_telefono"));
                cliente.setNumFax(rs.getString("num_fax"));
                cliente.setDesEmail(rs.getString("des_email"));
                cliente.setFecCreacion(rs.getString("fec_creacion"));
                cliente.setHorCreacion(rs.getString("hor_creacion"));
                cliente.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                cliente.setFecActualizacion(rs.getString("fec_actualizacion"));
                cliente.setHorActualizacion(rs.getString("hor_actualizacion"));
                cliente.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return cliente;

    }

}

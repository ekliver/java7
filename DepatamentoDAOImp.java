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
import sys.dao.DepartamentoDAO;
import sys.model.AgmaeDepartamento;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class DepatamentoDAOImp implements DepartamentoDAO {

    @Override
    public List<AgmaeDepartamento> listarDepartamentos() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeDepartamento Departamento = null;
        List<AgmaeDepartamento> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT cod_departamento, des_departamento, "
                    + "ces_departamento, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion "
                    + "FROM agmae_departamento;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                Departamento = new AgmaeDepartamento();
                Departamento.setCodDepartamento(rs.getInt("cod_departamento"));
                Departamento.setDesDepartamento(rs.getString("des_departamento"));
                Departamento.setCesDepartamento(rs.getString("ces_departamento"));
                Departamento.setFecCreacion(rs.getString("fec_creacion"));
                Departamento.setHorCreacion(rs.getString("hor_creacion"));
                Departamento.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                Departamento.setFecActualizacion(rs.getString("fec_actualizacion"));
                Departamento.setHorActualizacion(rs.getString("hor_actualizacion"));
                Departamento.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

                lista.add(Departamento);
            }
            if (!existe) {
                Departamento = null;
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
    public AgmaeDepartamento consultarObjDepartamento(int codDepartamento) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeDepartamento Departamento = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT cod_departamento, des_departamento, "
                    + "ces_departamento, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion "
                    + "FROM agmae_departamento "
                    + "WHERE cod_departamento=?;";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, codDepartamento);
            rs = ps.executeQuery();

            Departamento = new AgmaeDepartamento();
            if (rs.next()) {
                Departamento.setCodDepartamento(rs.getInt("cod_departamento"));
                Departamento.setDesDepartamento(rs.getString("des_departamento"));
                Departamento.setCesDepartamento(rs.getString("ces_departamento"));
                Departamento.setFecCreacion(rs.getString("fec_creacion"));
                Departamento.setHorCreacion(rs.getString("hor_creacion"));
                Departamento.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                Departamento.setFecActualizacion(rs.getString("fec_actualizacion"));
                Departamento.setHorActualizacion(rs.getString("hor_actualizacion"));
                Departamento.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
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
        return Departamento;

    }

}

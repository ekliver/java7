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
import sys.dao.LocalidadDAO;
import sys.model.AgmaeArea;
import sys.model.AgmaeEstablecimiento;
import sys.model.AgmaeUsuario;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class LocalidadDAOImp implements LocalidadDAO {

    @Override
    public List<AgmaeEstablecimiento> listarEstablecimientos(AgmaeUsuario usuario) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeEstablecimiento localidad = null;
        List<AgmaeEstablecimiento> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, COD_tipo_establecimiento, "
                    + "COD_establecimiento, NOM_establecimiento, flg_AbrevActivoFijo "
                    + "FROM AGMAE_Establecimiento "
                    + "WHERE ruc_companyia=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, usuario.getRucCompanyia());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                localidad = new AgmaeEstablecimiento();
                localidad.setRucCompanyia(rs.getString("ruc_companyia"));
                localidad.setCodTipoEstablecimiento(rs.getString("COD_tipo_establecimiento"));
                localidad.setCodEstablecimiento(rs.getString("COD_establecimiento"));
                localidad.setNomEstablecimiento(rs.getString("NOM_establecimiento"));
                localidad.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));

                lista.add(localidad);
            }
            if (!existe) {
                localidad = null;
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
    public AgmaeEstablecimiento consultarObjEstablecimiento(String rucCompanya, String codEstablecimiento) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeEstablecimiento localidad = null;
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, COD_tipo_establecimiento, "
                    + "COD_establecimiento, NOM_establecimiento, flg_AbrevActivoFijo "
                    + "FROM AGMAE_Establecimiento "
                    + "WHERE ruc_companyia=? AND COD_establecimiento=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);
            ps.setString(2, codEstablecimiento);
            rs = ps.executeQuery();

            localidad = new AgmaeEstablecimiento();
            if (rs.next()) {
                localidad.setRucCompanyia(rs.getString("ruc_companyia"));
                localidad.setCodTipoEstablecimiento(rs.getString("COD_tipo_establecimiento"));
                localidad.setCodEstablecimiento(rs.getString("COD_establecimiento"));
                localidad.setNomEstablecimiento(rs.getString("NOM_establecimiento"));
                localidad.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));

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
        return localidad;

    }

}

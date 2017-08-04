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
import sys.dao.AlmacenDAO;
import sys.dao.AreaDAO;
import sys.dao.CentroCostoDAO;
import sys.dao.LocalidadDAO;
import sys.dao.UsuarioDAO;
import sys.model.AgmaeUsuario;
import sys.util.Service;

/**
 *
 * @author cocotin
 */
public class UsuarioDAOImp implements UsuarioDAO {

    @Override
    public AgmaeUsuario obtenerDatosPorUsuario(AgmaeUsuario usuario) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeUsuario u = new AgmaeUsuario();
        List<AgmaeUsuario> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT agmae_usuario.cod_usuario AS codUsuario, "
                    + "agmae_usuario.nom_usuario AS nomUsuario, "
                    + "agmae_usuario.est_actividad AS estActividad, "
                    + "agmae_usuario.login_usuario AS loginUsuario, "
                    + "agmae_usuario.clave_usuario AS claveUsuario, "
                    + "AIMAR_ValoresxUsuario.ruc_Companyia AS rucCompanyia, "
                    + "AIMAR_ValoresxUsuario.cod_Establecimiento AS codEstablecimiento, "
                    + "AIMAR_ValoresxUsuario.cod_Centroc AS codCentro, "
                    + "AIMAR_ValoresxUsuario.cod_Area AS codArea, "
                    + "AIMAR_ValoresxUsuario.cod_Almacen AS codAlmacen "
                    + "FROM AIMAR_ValoresxUsuario RIGHT JOIN agmae_usuario ON AIMAR_ValoresxUsuario.cod_Usuario = agmae_usuario.cod_usuario\n"
                    + "WHERE (((agmae_usuario.login_usuario)=[?]) AND ((agmae_usuario.clave_usuario)=[?]));";
            ps = cn.prepareStatement(sql);
            ps.setString(1, usuario.getLoginUsuario());
            ps.setString(2, usuario.getClaveUsuario());
            rs = ps.executeQuery();

            if (rs.next()) {
                existe = true;
                u.setCodUsuario(rs.getInt("codUsuario"));
                u.setNomUsuario(rs.getString("nomUsuario"));
                u.setEstActividad(rs.getString("estActividad"));
                u.setLoginUsuario(rs.getString("loginUsuario"));
                u.setClaveUsuario(rs.getString("claveUsuario"));
                u.setRucCompanyia(rs.getString("rucCompanyia"));
                u.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                u.setCodCentroc(rs.getInt("codCentro"));
                u.setCodArea(rs.getInt("codArea"));
                u.setCodAlmacen(rs.getInt("codAlmacen"));

                LocalidadDAO lDao = new LocalidadDAOImp();
                CentroCostoDAO cCDao = new CentroCostoDAOImp();
                AreaDAO areaDao = new AreaDAOImp();
                AlmacenDAO almDao = new AlmacenDAOImp();

                u.setAgmaeEstablecimiento(lDao.consultarObjEstablecimiento(u.getRucCompanyia(), u.getCodEstablecimiento()));
                u.setAgmaeCentrocosto(cCDao.consultarObjCentroCosto(u.getCodCentroc()));
                u.setAgmaeArea(areaDao.consultarObjArea(u.getRucCompanyia(), u.getCodArea()));
                u.setAimarAlmacen(almDao.consultarObjAlmacen(u.getRucCompanyia(), u.getCodAlmacen()));

            }
            if (!existe) {
                u = null;
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
        return u;

    }

    @Override
    public AgmaeUsuario login(AgmaeUsuario usuario) {
        AgmaeUsuario user = this.obtenerDatosPorUsuario(usuario);
        if (user != null) {
            if (!user.getClaveUsuario().equals(usuario.getClaveUsuario())) {
                user = null;
            }
        }
        return user;
    }

}
